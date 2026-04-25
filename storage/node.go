package storage

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/replication"
	"github.com/satyam709/distributed-fs/storage/server"
	"github.com/satyam709/distributed-fs/storage/service"
	"github.com/satyam709/distributed-fs/storage/store"
	"google.golang.org/grpc"
)

var _ service.NodeInfo = (*StorageNode)(nil)

// StorageNode is the top-level runtime for a storage service instance.
// It owns the gRPC server lifecycle and wires the StorageService,
// ReplicationService, PeerDialer, and Replication Manager together.
type StorageNode struct {
	config             StorageNodeConfig
	logger             *logging.CLogger
	store              store.Store
	metaClient         metaclient.StorageMetadataClientInterface
	grpcServer         *grpc.Server
	peerDialer         *replication.PeerDialer
	replicationManager *replication.ReplicationManager
	registerer         service.NodeRegisterer
	heartbeatSender    *service.HeartbeatSender
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
	if metaClient == nil {
		return nil, errors.New("StorageNode: metaClient must not be nil")
	}

	dialer := replication.NewPeerDialer()
	manager := replication.NewReplicationManager(dialer, store, metaClient)

	return &StorageNode{
		config:             cfg,
		store:              store,
		logger:             logger,
		metaClient:         metaClient,
		peerDialer:         dialer,
		replicationManager: manager,
	}, nil
}

// GetNodeID returns the unique identifier for this storage node.
// Implements the heartbeat.NodeInfo interface.
func (s *StorageNode) GetNodeID() string {
	return s.config.NodeID
}

// GetFreeSpace returns the available disk space in bytes.
// Implements the heartbeat.NodeInfo interface.
func (s *StorageNode) GetFreeSpace() uint64 {
	space, err := s.store.FreeSpace()
	if err != nil {
		s.logger.Warn("failed to get free space", slog.String("err", err.Error()))
		return 0
	}
	return space
}

// GetChunkCount returns the number of chunks currently stored on this node.
// Implements the heartbeat.NodeInfo interface.
func (s *StorageNode) GetChunkCount() uint32 {
	chunks, err := s.store.List()
	if err != nil {
		s.logger.Warn("failed to list chunks", slog.String("err", err.Error()))
		return 0
	}
	return uint32(len(chunks))
}

// GetChunkList returns the IDs of chunks currently stored on this node.
// Implements the service.NodeInfo interface.
func (s *StorageNode) GetChunkList() ([]string, error) {
	return s.store.List()
}

// GetGrpcAddr returns this node's gRPC bind address.
// Implements the service.NodeInfo interface.
func (s *StorageNode) GetGrpcAddr() string {
	return s.config.GRPCAddr
}

// Start binds the TCP listener, wires gRPC services, and launches the server
// in a goroutine. It returns immediately; use Stop to initiate graceful shutdown.
func (s *StorageNode) Start() error {
	if err := s.config.Validate(); err != nil {
		return err
	}

	ctx := context.Background()
	s.registerer = service.NewStorageNodeRegisterer(s, s.metaClient)

	if err := s.registerer.RegisterWithMetadata(ctx); err != nil {
		s.logger.Warn("StorageNode: failed to register with metadata, will retry on heartbeat",
			slog.String("err", err.Error()))
	}

	s.heartbeatSender = service.NewHeartbeatSender(
		s.config.HeartbeatInterval,
		s.metaClient,
		s,
		s.replicationManager,
		s.registerer,
	)

	s.logger.Info("StorageNode: binding TCP listener", slog.String("addr", s.config.GRPCAddr))

	listener, err := net.Listen("tcp", s.config.GRPCAddr)
	if err != nil {
		s.logger.Error("StorageNode: failed to bind listener", err,
			slog.String("addr", s.config.GRPCAddr))
		return err
	}

	s.grpcServer = grpc.NewServer(grpc.ConnectionTimeout(s.config.Timeout))

	storageServer, err := server.NewStorageServerHandler(s.store, s.logger, s.metaClient, s.config.NodeID, s.replicationManager)
	if err != nil {
		return err
	}
	pb_storage.RegisterStorageServiceServer(s.grpcServer, storageServer)

	replServer, err := server.NewReplicationServerHandler(s.store, s.logger)
	if err != nil {
		return err
	}
	pb_storage.RegisterReplicationServiceServer(s.grpcServer, replServer)

	s.replicationManager.Start()

	s.logger.Info("StorageNode: gRPC server starting",
		slog.String("addr", s.config.GRPCAddr),
		slog.Duration("connectionTimeout", s.config.Timeout),
	)

	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			s.logger.Error("StorageNode: gRPC server exited with error", err)
		}
	}()

	s.heartbeatSender.Start(ctx)

	s.logger.Info("StorageNode: server is up and accepting connections",
		slog.String("addr", s.config.GRPCAddr),
		slog.String("nodeID", s.config.NodeID))
	return nil
}

// Stop initiates a graceful shutdown of the gRPC server, repair workers,
// heartbeat sender, and peer connections.
func (s *StorageNode) Stop() {
	if s.grpcServer == nil {
		return
	}
	s.logger.Info("StorageNode: initiating graceful shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if s.heartbeatSender != nil {
		s.heartbeatSender.StopAndWait(ctx)
	}

	if s.registerer != nil {
		if err := s.registerer.DeregisterFromMetadata(ctx); err != nil {
			s.logger.Warn("StorageNode: failed to deregister from metadata",
				slog.String("err", err.Error()))
		}
	}

	s.grpcServer.GracefulStop()
	s.replicationManager.Stop()
	s.peerDialer.CloseAll()
	s.logger.Info("StorageNode: stopped")
}
