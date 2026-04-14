package server

import (
	"context"
	"time"

	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
)

func (h *MetadataServiceHandler) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	return nil, nil
}

func (h *MetadataServiceHandler) DeregisterNode(ctx context.Context, req *pb.DeregisterNodeRequest) (*pb.DeregisterNodeResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	return nil, nil
}

func (h *MetadataServiceHandler) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	// 0. Count the heartbeat
	h.mu.Lock()
	h.heartbeatsCount[req.NodeId]++
	count := h.heartbeatsCount[req.NodeId]
	h.mu.Unlock()
	// 1. NodeWatcher.UpdateLastSeen(nodeID, now) — in memory, not Raft
	h.nodeWatcher.UpdateLastSeen(req.NodeId, time.Now())
	// 2. Every 10th heartbeat: propose CmdUpdateNodeSpace
	if count%10 == 0 {
		err := fsm.ProposeUpdateNodeSpace(h.raft, req.NodeId, uint64(req.FreeSpace), uint64(req.ChunkCount))
		if err != nil {
			h.logger.Warn("failed to update the free-space", "err", err.Error())
		}
	}
	// 3. Check RepairScheduler pending jobs for this node
	pjobs := h.repairer.GetPendingJobsForNode(req.NodeId)

	// 4. Return pending repair instructions
	ins := []*pb.RepairInstruction{}
	for _, jobID := range pjobs {
		val, err := h.fsm.GetRepairJob(jobID)
		if err != nil {
			h.logger.Warn("a job cant be fetched", "err", err.Error())
		}
		ins = append(ins, &pb.RepairInstruction{
			JobId:        val.JobID,
			ChunkId:      val.ChunkID,
			TargetNodeId: val.TargetNodeID,
			TargetAddr:   "", // TODO: FIX this
			// And Add source details too
		})
	}
	return &pb.HeartbeatResponse{RepairJobs: ins}, nil
}
