package server

import (
	"github.com/hashicorp/raft"
	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/raftutil"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MetadataServiceHandler struct {
	pb.UnimplementedMetadataServiceServer
	raft *raft.Raft
	fsm  *fsm.MetadataFSM
}

func NewMetadataServiceHandler(r *raft.Raft, f *fsm.MetadataFSM) *MetadataServiceHandler {
	return &MetadataServiceHandler{
		raft: r,
		fsm:  f,
	}
}

// isLeader checks if the current node is the leader of the Raft cluster.
func (h *MetadataServiceHandler) isLeader() bool {
	return raftutil.IsLeader(h.raft)
}

func (h *MetadataServiceHandler) leaderRedirect() error {
	addr := raftutil.LeaderAddress(h.raft)
	if addr == "" {
		return status.Error(codes.Unavailable, "no leader elected yet")
	}
	return status.Errorf(codes.FailedPrecondition, "not leader, redirect to : %s", addr)
}
