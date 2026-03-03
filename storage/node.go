package storage

import (
	"net"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/server"
	"github.com/satyam709/distributed-fs/storage/store"
	"google.golang.org/grpc"
)

type StorageNode struct {
	config     StorageNodeConfig
	logger     *logging.CLogger
	store      store.Store
	grpcServer *grpc.Server
}

func NewStorageNode(cfg StorageNodeConfig, logger *logging.CLogger, store store.Store) *StorageNode {
	return &StorageNode{config: cfg, store: store, logger: logger}
}

func (s *StorageNode) Start() error {
	listener, err := net.Listen("tcp", s.config.Port)
	if err != nil {
		return err
	}
	s.grpcServer = grpc.NewServer(grpc.ConnectionTimeout(s.config.Timeout))

	storageServer := server.StorageServer{Store: s.store}
	pb_storage.RegisterStorageServiceServer(s.grpcServer, storageServer)

	// start the server in a go-routine
	go func() {
		err := s.grpcServer.Serve(listener)
		if err != nil {
			s.logger.Error("failed to serve", err)
			return
		}
	}()
	return nil
}
func (s *StorageNode) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
}
