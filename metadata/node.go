package metadata

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
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

// MetadataNode is the top-level runtime for a metadata service instance.
// It owns the Raft FSM, the gRPC server lifecycle, and all background
// workers (NodeWatcher, RepairScheduler, Reconciler).
type MetadataNode struct {
	Config       NodeConfig
	logger       *logging.CLogger
	stableStore  raft.StableStore
	logStore     raft.LogStore
	fsm          *fsm.MetadataFSM
	raftInstance *raft.Raft
	watcher      *watcher.NodeWatcher
	scheduler    *scheduler.RepairScheduler
	reconciler   *reconcile.Reconciler
	grpcServer   *grpc.Server
	grpcLis      net.Listener
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

// NewMetadataNode initialises all metadata-node components in dependency
// order: BoltStore → FSM → Raft → Scheduler → Watcher → Reconciler.
// Call Start() to begin serving.
func NewMetadataNode(config NodeConfig) (*MetadataNode, error) {
	mfsm := fsm.NewEmptyMetadataFsm(logging.NewCLogger())

	info, err := os.Stat(config.RaftDir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("NewMetadataNode: raftDir does not exist")
	}

	boltStore, err := raftboltdb.New(
		raftboltdb.Options{
			Path: filepath.Join(config.RaftDir, "raft.db"),
		},
	)
	if err != nil {
		return nil, err
	}

	stableStore := boltStore
	logStore := boltStore

	raftIns, err := NewRaftNode(RaftConfig{
		Config:      config,
		FSM:         mfsm,
		LogStore:    logStore,
		StableStore: stableStore,
	})
	if err != nil {
		return nil, err
	}

	proposer := &raftProposer{raft: raftIns}

	sourceStrategy := placement.LeastLoadedStrategy{}
	targetStrategy := placement.MostFreeSpaceStrategy{}

	rf := config.ReplicationFactor
	if rf <= 0 {
		rf = 3
	}

	repairScheduler := scheduler.NewRepairScheduler(raftIns, mfsm, sourceStrategy, targetStrategy, rf)

	nodeWatcher := watcher.NewNodeWatcher(
		mfsm,
		proposer,
		repairScheduler,
		config.SuspectTimeout,
		config.WatcherInterval,
		logging.NewCLogger().With("component", "NodeWatcher"),
	)

	storageClient := reconcile.NewThinStorageClient()
	reconciler := reconcile.NewReconciler(
		mfsm,
		proposer,
		repairScheduler,
		storageClient,
		config.ReconcileDelay,
		rf,
		logging.NewCLogger().With("component", "Reconciler"),
	)

	return &MetadataNode{
		Config:       config,
		logger:       logging.NewCLogger().With("component", "MetadataNode"),
		stableStore:  stableStore,
		logStore:     logStore,
		fsm:          mfsm,
		raftInstance: raftIns,
		watcher:      nodeWatcher,
		scheduler:    repairScheduler,
		reconciler:   reconciler,
	}, nil
}

// Start waits for a Raft leader to emerge, starts background workers,
// and launches the gRPC server. It returns once the server is listening.
func (mn *MetadataNode) Start(ctx context.Context) error {
	mn.logger.Info("starting metadata node")

	if err := raftutil.WaitForLeader(mn.raftInstance, 30*time.Second); err != nil {
		return err
	}

	mn.scheduler.Start(ctx)
	go mn.watcher.Start()

	lis, err := net.Listen("tcp", mn.Config.GRPCAddr)
	if err != nil {
		return err
	}
	mn.grpcLis = lis

	mn.grpcServer = server.NewGRPCServer(mn.raftInstance, mn.fsm, mn.watcher, mn.scheduler, mn.reconciler)
	go func() {
		if err := mn.grpcServer.Serve(lis); err != nil {
			mn.logger.Error("gRPC server exited", err)
		}
	}()

	mn.logger.Info("metadata node started", "grpcAddr", mn.Config.GRPCAddr)
	return nil
}

// Shutdown gracefully stops the gRPC server, background workers, Raft,
// and closes persistent stores.
func (mn *MetadataNode) Shutdown(ctx context.Context) error {
	mn.logger.Info("stopping metadata node")

	if mn.grpcServer != nil {
		mn.grpcServer.GracefulStop()
	}

	mn.watcher.Stop()
	mn.scheduler.Stop()

	if err := mn.raftInstance.Shutdown().Error(); err != nil {
		mn.logger.Error("raft shutdown error", err)
	}

	if closer, ok := mn.stableStore.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			mn.logger.Error("bolt store close error", err)
		}
	}

	mn.logger.Info("metadata node stopped")
	return nil
}
