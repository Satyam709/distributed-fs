package server

import (
	"fmt"
	"net"

	"github.com/hashicorp/raft"
	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/placement"
	"github.com/satyam709/distributed-fs/metadata/reconcile"
	"github.com/satyam709/distributed-fs/metadata/scheduler"
	"github.com/satyam709/distributed-fs/metadata/watcher"
	"google.golang.org/grpc"
)

// NewGRPCServer builds a gRPC server with the MetadataService handler wired
// to the given Raft FSM, watcher, scheduler and reconciler.
func NewGRPCServer(r *raft.Raft, f *fsm.MetadataFSM, nw *watcher.NodeWatcher, re *scheduler.RepairScheduler, rec *reconcile.Reconciler, ps placement.PlacementStrategy, rc int) *grpc.Server {
	grpcServer := grpc.NewServer()
	handler := NewMetadataServiceHandler(r, f, re, nw, rec, ps, rc)
	pb.RegisterMetadataServiceServer(grpcServer, handler)
	return grpcServer
}

// StartGRPCServer is a convenience wrapper that binds a TCP listener,
// registers the MetadataService handler, and blocks serving requests.
// For graceful shutdown use NewGRPCServer and call Serve on the returned
// server yourself.
func StartGRPCServer(addr string, r *raft.Raft, f *fsm.MetadataFSM, nw *watcher.NodeWatcher, re *scheduler.RepairScheduler, rec *reconcile.Reconciler, ps placement.PlacementStrategy, rc int) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	grpcServer := NewGRPCServer(r, f, nw, re, rec, ps, rc)
	return grpcServer.Serve(lis)
}
