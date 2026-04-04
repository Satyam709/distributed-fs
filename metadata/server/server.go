package server

import (
	"fmt"
	"net"

	"github.com/hashicorp/raft"
	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"google.golang.org/grpc"
)

func StartGRPCServer(addr string, r *raft.Raft, f *fsm.MetadataFSM) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	handler := NewMetadataServiceHandler(r, f)
	pb.RegisterMetadataServiceServer(grpcServer, handler)

	return grpcServer.Serve(lis)
}
