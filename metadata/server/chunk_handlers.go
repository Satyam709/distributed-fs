package server

import (
	"context"
	"time"

	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *MetadataServiceHandler) CommitChunk(ctx context.Context, req *pb.CommitChunkRequest) (*pb.CommitChunkResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect(ctx)
	}

	cmd, err := newCommand(fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID:  req.ChunkId,
		NodeIDs:  req.ConfirmedNodes,
		Checksum: req.Checksum,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating commit-chunk command: %v", err)
	}

	if err := fsm.Propose(h.deps.Raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing commit-chunk command: %v", err)
	}

	locations, err := h.deps.FSM.GetChunkLocations(req.ChunkId)
	if err != nil {
		h.deps.Logger.Warn("cannot check chunk locations after commit", "chunkID", req.ChunkId, "err", err.Error())
	} else if len(locations) < h.deps.ReplicationFactor {
		h.deps.Scheduler.ScheduleRepairForChunk(req.ChunkId)
	}

	return &pb.CommitChunkResponse{Success: true}, nil
}

func (h *MetadataServiceHandler) GetChunkLocations(ctx context.Context, req *pb.GetChunkLocationsRequest) (*pb.GetChunkLocationsResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect(ctx)
	}

	nodes, err := h.deps.FSM.GetChunkLocations(req.ChunkId)
	if err != nil {
		if err == fsm.ErrChunkNotFound {
			return nil, status.Errorf(codes.NotFound, "chunk not found: %s", req.ChunkId)
		}
		return nil, status.Errorf(codes.Internal, "error fetching chunk locations: %v", err)
	}

	pbNodes := make([]*pb.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		pbNodes = append(pbNodes, &pb.NodeInfo{
			NodeId:    n.NodeID,
			Address:   n.Address,
			FreeSpace: int64(n.FreeSpace),
		})
	}

	return &pb.GetChunkLocationsResponse{Nodes: pbNodes}, nil
}

func (h *MetadataServiceHandler) ReportCorruption(ctx context.Context, req *pb.ReportCorruptionRequest) (*pb.ReportCorruptionResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect(ctx)
	}

	cmd, err := newCommand(fsm.CmdEvictChunkFromNode, fsm.CommandEvictChunkFromNode{
		ChunkID:   req.ChunkId,
		NodeID:    req.ReporterId,
		EvictedAt: time.Now(),
		Reason:    "corruption reported by node",
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating eviction command: %v", err)
	}

	if err := fsm.Propose(h.deps.Raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing eviction command: %v", err)
	}

	// If chunk is now under-replicated, schedule repair.
	h.deps.Scheduler.ScheduleRepairForChunk(req.ChunkId)

	return &pb.ReportCorruptionResponse{Success: true}, nil
}
