package server

import (
	"fmt"
	"net"
	"time"

	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func NewGRPCServer(deps *HandlerDeps) *grpc.Server {
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Minute,
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
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
