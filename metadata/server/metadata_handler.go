package server

import (
	"sync"

	"github.com/hashicorp/raft"
	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/scheduler"
	"github.com/satyam709/distributed-fs/metadata/watcher"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MetadataServiceHandler struct {
	pb.UnimplementedMetadataServiceServer
	raft            *raft.Raft
	fsm             *fsm.MetadataFSM
	repairer        *scheduler.RepairScheduler
	nodeWatcher     *watcher.NodeWatcher
	mu              sync.Mutex
	heartbeatsCount map[string]int
	logger          *logging.CLogger
}

func NewMetadataServiceHandler(r *raft.Raft, f *fsm.MetadataFSM, re *scheduler.RepairScheduler, nw *watcher.NodeWatcher) *MetadataServiceHandler {
	return &MetadataServiceHandler{
		raft:            r,
		fsm:             f,
		heartbeatsCount: map[string]int{},
		nodeWatcher:     nw,
		repairer:        re,
		logger:          logging.NewCLogger().With("component", "metadata-handler"),
	}
}

// isLeader checks if the current node is the leader of the Raft cluster.
func (h *MetadataServiceHandler) isLeader() bool {
	return metadata.IsLeader(h.raft)
}

func (h *MetadataServiceHandler) leaderRedirect() error {
	addr := metadata.LeaderAddress(h.raft)
	if addr == "" {
		return status.Error(codes.Unavailable, "no leader elected yet")
	}
	return status.Errorf(codes.FailedPrecondition, "not leader, redirect to : %s", addr)
}
