package server

import (
	"context"
	"sync"

	"github.com/hashicorp/raft"
	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/internal/raftutil"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/placement"
	"github.com/satyam709/distributed-fs/metadata/reconcile"
	"github.com/satyam709/distributed-fs/metadata/scheduler"
	"github.com/satyam709/distributed-fs/metadata/watcher"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type HandlerDeps struct {
	Raft              *raft.Raft
	FSM               *fsm.MetadataFSM
	Scheduler         *scheduler.RepairScheduler
	Watcher           *watcher.NodeWatcher
	Reconciler        *reconcile.Reconciler
	TargetPlacement   placement.PlacementStrategy
	Logger            *logging.CLogger
	ReplicationFactor int
}

type MetadataServiceHandler struct {
	pb.UnimplementedMetadataServiceServer
	deps *HandlerDeps

	mu              sync.Mutex
	heartbeatsCount map[string]int
}

func NewMetadataServiceHandler(deps *HandlerDeps) *MetadataServiceHandler {
	return &MetadataServiceHandler{
		deps:            deps,
		heartbeatsCount: map[string]int{},
	}
}

// isLeader checks if the current node is the leader of the Raft cluster.
func (h *MetadataServiceHandler) isLeader() bool {
	return raftutil.IsLeader(h.deps.Raft)
}

// leaderRedirect returns a gRPC error indicating the client should connect
// to the current Raft leader instead. It sets the x-leader-grpc-addr trailer
// so the client can extract the leader address without string parsing.
func (h *MetadataServiceHandler) leaderRedirect(ctx context.Context) error {
	addr := raftutil.LeaderAddress(h.deps.Raft)
	if addr == "" {
		return status.Error(codes.Unavailable, "no leader elected yet")
	}
	grpcAddr, err := h.deps.FSM.GetMetadataNodeByRaftAddr(addr)
	if err == nil {
		grpc.SetTrailer(ctx, metadata.Pairs("x-leader-grpc-addr", grpcAddr.GrpcAddr))
	}
	return status.Error(codes.FailedPrecondition, "not leader")
}
