package server

import (
	"fmt"
	"net"

	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"google.golang.org/grpc"
)

func NewGRPCServer(deps *HandlerDeps) *grpc.Server {
	grpcServer := grpc.NewServer()
	handler := NewMetadataServiceHandler(deps)
	pb.RegisterMetadataServiceServer(grpcServer, handler)
	return grpcServer
}

func StartGRPCServer(addr string, deps *HandlerDeps) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	grpcServer := NewGRPCServer(deps)
	return grpcServer.Serve(lis)
}
