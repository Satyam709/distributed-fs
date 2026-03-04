package storage

import (
	"errors"
	"log/slog"
	"net"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/server"
	"github.com/satyam709/distributed-fs/storage/store"
	"google.golang.org/grpc"
)

// StorageNode is the top-level runtime for a storage service instance.
// It owns the gRPC server lifecycle and delegates requests to the StorageServer.
type StorageNode struct {
	config     StorageNodeConfig
	logger     *logging.CLogger
	store      store.Store
	grpcServer *grpc.Server
}

// NewStorageNode constructs a StorageNode. Returns an error if store or logger
// is nil. Call Start to bind and begin serving.
func NewStorageNode(cfg StorageNodeConfig, logger *logging.CLogger, store store.Store) (*StorageNode, error) {
	if store == nil {
		return nil, errors.New("StorageNode: store must not be nil")
	}
	if logger == nil {
		return nil, errors.New("StorageNode: logger must not be nil")
	}
	return &StorageNode{config: cfg, store: store, logger: logger}, nil
}

// Start binds the TCP listener and launches the gRPC server in a goroutine.
// It returns immediately; use Stop to initiate a graceful shutdown.
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

	storageServer, err := server.NewStorageServer(s.store, s.logger)
	if err != nil {
		return err
	}
	pb_storage.RegisterStorageServiceServer(s.grpcServer, storageServer)

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

// Stop initiates a graceful shutdown of the gRPC server. Pending RPCs are
// allowed to complete; new connections are rejected immediately.
func (s *StorageNode) Stop() {
	if s.grpcServer == nil {
		return
	}
	s.logger.Info("StorageNode: initiating graceful shutdown")
	s.grpcServer.GracefulStop()
	s.logger.Info("StorageNode: stopped")
}
