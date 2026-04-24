package metadata

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/internal/raftutil"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/placement"
	"github.com/satyam709/distributed-fs/metadata/reconcile"
	"github.com/satyam709/distributed-fs/metadata/scheduler"
	"github.com/satyam709/distributed-fs/metadata/server"
	"github.com/satyam709/distributed-fs/metadata/watcher"
	"google.golang.org/grpc"
)

type MetadataApp struct {
	Config *NodeConfig

	logger *logging.CLogger
	fsm    *fsm.MetadataFSM
	raft   *raft.Raft
	store  *raftboltdb.BoltStore

	watcher    *watcher.NodeWatcher
	scheduler  *scheduler.RepairScheduler
	reconciler *reconcile.Reconciler

	grpcServer *grpc.Server
	grpcLis    net.Listener

	sourcePlacement placement.PlacementStrategy
	targetPlacement placement.PlacementStrategy
	storageClient   reconcile.StorageClient
}

type AppOption func(*MetadataApp)

func WithLogger(logger *logging.CLogger) AppOption {
	return func(a *MetadataApp) {
		a.logger = logger
	}
}

func WithSourcePlacement(strategy placement.PlacementStrategy) AppOption {
	return func(a *MetadataApp) {
		a.sourcePlacement = strategy
	}
}

func WithTargetPlacement(strategy placement.PlacementStrategy) AppOption {
	return func(a *MetadataApp) {
		a.targetPlacement = strategy
	}
}

func WithStorageClient(client reconcile.StorageClient) AppOption {
	return func(a *MetadataApp) {
		a.storageClient = client
	}
}

func NewMetadataApp(config NodeConfig, opts ...AppOption) (*MetadataApp, error) {
	config.Default()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	app := &MetadataApp{
		Config:          &config,
		logger:          logging.NewCLogger().With("component", "MetadataApp"),
		sourcePlacement: placement.LeastLoadedStrategy{},
		targetPlacement: placement.MostFreeSpaceStrategy{},
	}

	for _, opt := range opts {
		opt(app)
	}

	if err := app.initFSM(); err != nil {
		return nil, err
	}

	if err := app.initStore(); err != nil {
		return nil, err
	}

	if err := app.initRaft(); err != nil {
		return nil, err
	}

	app.initWorkers()

	return app, nil
}

func (a *MetadataApp) initFSM() error {
	a.fsm = fsm.NewEmptyMetadataFsm(a.logger)
	return nil
}

func (a *MetadataApp) initStore() error {
	info, err := os.Stat(a.Config.RaftDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("raftDir does not exist")
	}

	boltStore, err := raftboltdb.New(
		raftboltdb.Options{
			Path: filepath.Join(a.Config.RaftDir, "raft.db"),
		},
	)
	if err != nil {
		return err
	}
	a.store = boltStore
	return nil
}

func (a *MetadataApp) initRaft() error {
	raftIns, err := NewRaftNode(RaftConfig{
		Config:      *a.Config,
		FSM:         a.fsm,
		LogStore:    a.store,
		StableStore: a.store,
	})
	if err != nil {
		return err
	}
	a.raft = raftIns
	return nil
}

func (a *MetadataApp) initWorkers() {
	proposer := &raftProposer{raft: a.raft}
	rf := a.Config.ReplicationCount()

	a.scheduler = scheduler.NewRepairScheduler(
		a.raft,
		a.fsm,
		a.sourcePlacement,
		a.targetPlacement,
		rf,
	)

	a.watcher = watcher.NewNodeWatcher(
		a.fsm,
		proposer,
		a.scheduler,
		a.Config.SuspectTimeout,
		a.Config.WatcherInterval,
		a.logger.With("component", "NodeWatcher"),
	)

	if a.storageClient == nil {
		a.storageClient = reconcile.NewThinStorageClient()
	}
	a.reconciler = reconcile.NewReconciler(
		a.fsm,
		proposer,
		a.scheduler,
		a.storageClient,
		a.Config.ReconcileDelay,
		rf,
		a.logger.With("component", "Reconciler"),
	)
}

func (a *MetadataApp) Run(ctx context.Context) error {
	a.logger.Info("starting metadata app", "nodeID", a.Config.NodeID)

	if err := raftutil.WaitForLeader(a.raft, 30*time.Second); err != nil {
		return err
	}

	a.scheduler.Start(ctx)
	go a.watcher.Start()

	lis, err := net.Listen("tcp", a.Config.GRPCAddr)
	if err != nil {
		return err
	}
	a.grpcLis = lis

	deps := &server.HandlerDeps{
		Raft:              a.raft,
		FSM:               a.fsm,
		Scheduler:         a.scheduler,
		Watcher:           a.watcher,
		Reconciler:        a.reconciler,
		TargetPlacement:   a.targetPlacement,
		Logger:            a.logger,
		ReplicationFactor: a.Config.ReplicationCount(),
	}
	a.grpcServer = server.NewGRPCServer(deps)
	go func() {
		if err := a.grpcServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			a.logger.Error("gRPC server exited", err)
		}
	}()

	a.logger.Info("metadata app started", "grpcAddr", a.Config.GRPCAddr)
	return nil
}

func (a *MetadataApp) Shutdown(ctx context.Context) error {
	a.logger.Info("stopping metadata app")

	if a.grpcServer != nil {
		a.grpcServer.GracefulStop()
	}

	a.watcher.Stop()
	a.scheduler.Stop()

	if a.raft != nil {
		if err := a.raft.Shutdown().Error(); err != nil {
			a.logger.Error("raft shutdown error", err)
		}
	}

	if a.store != nil {
		if err := a.store.Close(); err != nil {
			a.logger.Error("bolt store close error", err)
		}
	}

	a.logger.Info("metadata app stopped")
	return nil
}

// raftProposer wraps a *raft.Raft to satisfy the watcher.Proposer and
// reconcile.CommandProposer interfaces.
type raftProposer struct {
	raft *raft.Raft
}

func (rp *raftProposer) IsLeader() bool {
	return raftutil.IsLeader(rp.raft)
}

func (rp *raftProposer) Propose(cmd fsm.MetadataCommand) error {
	return fsm.Propose(rp.raft, cmd)
}
