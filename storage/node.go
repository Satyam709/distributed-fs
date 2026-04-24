package storage

import (
	"errors"
	"log/slog"
	"net"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/replication"
	"github.com/satyam709/distributed-fs/storage/server"
	"github.com/satyam709/distributed-fs/storage/store"
	"google.golang.org/grpc"
)

// StorageNode is the top-level runtime for a storage service instance.
// It owns the gRPC server lifecycle and wires the StorageService,
// ReplicationService, PeerDialer, and Replication Manager together.
type StorageNode struct {
	config             StorageNodeConfig
	logger             *logging.CLogger
	store              store.Store
	grpcServer         *grpc.Server
	peerDialer         *replication.PeerDialer
	replicationManager *replication.ReplicationManager
}

// NewStorageNode constructs a StorageNode. Returns an error if store or logger
// is nil. Call Start to bind and begin serving.
func NewStorageNode(cfg StorageNodeConfig, logger *logging.CLogger, store store.Store, metaClient metaclient.StorageMetadataClientInterface) (*StorageNode, error) {
	if store == nil {
		return nil, errors.New("StorageNode: store must not be nil")
	}
	if logger == nil {
		return nil, errors.New("StorageNode: logger must not be nil")
	}

	dialer := replication.NewPeerDialer()
	manager := replication.NewReplicationManager(dialer, store, metaClient)

	return &StorageNode{
		config:             cfg,
		store:              store,
		logger:             logger,
		peerDialer:         dialer,
		replicationManager: manager,
	}, nil
}

// Start binds the TCP listener, wires gRPC services, and launches the server
// in a goroutine. It returns immediately; use Stop to initiate graceful shutdown.
func (s *StorageNode) Start() error {
	if err := s.config.Validate(); err != nil {
		return err
	}
	s.logger.Info("StorageNode: binding TCP listener", slog.String("addr", s.config.Port))

	listener, err := net.Listen("tcp", s.config.Port)
	if err != nil {
		s.logger.Error("StorageNode: failed to bind listener", err,
			slog.String("addr", s.config.Port))
		return err
	}

	s.grpcServer = grpc.NewServer(grpc.ConnectionTimeout(s.config.Timeout))

	// StorageService — client-facing RPC (PutChunk, GetChunk, etc.)
	storageServer, err := server.NewStorageServerHandler(s.store, s.logger, s.replicationManager)
	if err != nil {
		return err
	}
	pb_storage.RegisterStorageServiceServer(s.grpcServer, storageServer)

	// ReplicationService — internal P2P RPC (ReplicateChunk)
	replServer, err := server.NewReplicationServerHandler(s.store, s.logger)
	if err != nil {
		return err
	}
	pb_storage.RegisterReplicationServiceServer(s.grpcServer, replServer)

	// Start repair worker pool.
	s.replicationManager.Start()

	s.logger.Info("StorageNode: gRPC server starting",
		slog.String("addr", s.config.Port),
		slog.Duration("connectionTimeout", s.config.Timeout),
	)

	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			s.logger.Error("StorageNode: gRPC server exited with error", err)
		}
	}()

	s.logger.Info("StorageNode: server is up and accepting connections",
		slog.String("addr", s.config.Port))
	return nil
}

// Stop initiates a graceful shutdown of the gRPC server, repair workers,
// and peer connections.
func (s *StorageNode) Stop() {
	if s.grpcServer == nil {
		return
	}
	s.logger.Info("StorageNode: initiating graceful shutdown")
	s.grpcServer.GracefulStop()
	s.replicationManager.Stop()
	s.peerDialer.CloseAll()
	s.logger.Info("StorageNode: stopped")
}
