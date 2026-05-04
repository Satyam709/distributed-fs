package server

import (
	"context"
	"time"

	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type repairInstructionLookup interface {
	GetRepairJob(jobID string) (fsm.RepairJob, error)
	GetNode(nodeID string) (*fsm.NodeEntry, error)
}

func (h *MetadataServiceHandler) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect(ctx)
	}

	cmd, err := newCommand(fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID:     req.NodeId,
		Address:    req.Address,
		FreeSpace:  uint64(req.FreeSpace),
		ChunkCount: uint64(len(req.ChunkIds)),
		CreatedAt:  time.Now(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating register command: %v", err)
	}

	if err := fsm.Propose(h.deps.Raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing register command: %v", err)
	}

	// Schedule reconciliation after the configured delay. If the node
	// re-registers before the delay expires the old timer is cancelled.
	h.deps.Reconciler.Schedule(req.NodeId, req.ChunkIds)

	return &pb.RegisterNodeResponse{NodeId: req.NodeId}, nil
}

func (h *MetadataServiceHandler) DeregisterNode(ctx context.Context, req *pb.DeregisterNodeRequest) (*pb.DeregisterNodeResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect(ctx)
	}

	_, err := h.deps.FSM.GetNode(req.NodeId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "node not found: %s", req.NodeId)
	}

	cmd, err := newCommand(fsm.CmdDeregisterNode, fsm.CommandDeregisterNode{
		NodeID:    req.NodeId,
		UpdatedAt: time.Now(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating deregister command: %v", err)
	}

	if err := fsm.Propose(h.deps.Raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing deregister command: %v", err)
	}

	// Trigger repair for all chunks on this node immediately.
	h.deps.Scheduler.TriggerRepair(req.NodeId)

	return &pb.DeregisterNodeResponse{Success: true}, nil
}

func (h *MetadataServiceHandler) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect(ctx)
	}
	// -1 see if the node even exists if not signal it to register first
	_, err := h.deps.FSM.GetNode(req.NodeId)
	if err != nil {
		if err == fsm.ErrNodeNotFound {
			return nil, status.Error(codes.NotFound, "node not registered")
		}
		return nil, status.Error(codes.Internal, "internal")
	}
	// 0. Count the heartbeat
	h.mu.Lock()
	h.heartbeatsCount[req.NodeId]++
	count := h.heartbeatsCount[req.NodeId]
	h.mu.Unlock()
	// 1. NodeWatcher.UpdateLastSeen(nodeID, now) — in memory, not Raft
	if err := h.deps.Watcher.UpdateLastSeen(req.NodeId, time.Now()); err != nil {
		h.deps.Logger.Warn("failed to update node last seen", "nodeID", req.NodeId, "err", err.Error())
	}
	// 2. Every 10th heartbeat: propose CmdUpdateNodeSpace
	if count%10 == 0 {
		err := fsm.ProposeUpdateNodeSpace(h.deps.Raft, req.NodeId, uint64(req.FreeSpace), uint64(req.ChunkCount))
		if err != nil {
			h.deps.Logger.Warn("failed to update the free-space", "err", err.Error())
		}
	}
	// 3. Check RepairScheduler pending jobs for this node
	pjobs := h.deps.Scheduler.GetPendingJobsForNode(req.NodeId)

	// 4. Return pending repair instructions
	ins := buildRepairInstructions(h.deps.FSM, pjobs, h.deps.Logger)
	return &pb.HeartbeatResponse{RepairJobs: ins}, nil
}

func buildRepairInstructions(m repairInstructionLookup, jobIDs []string, logger interface {
	Warn(msg string, keyvals ...interface{})
}) []*pb.RepairInstruction {
	ins := make([]*pb.RepairInstruction, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		val, err := m.GetRepairJob(jobID)
		if err != nil {
			logger.Warn("repair job lookup failed", "jobID", jobID, "err", err.Error())
			continue
		}

		sourceAddr := ""
		if val.SourceNodeID != "" {
			sourceNode, err := m.GetNode(val.SourceNodeID)
			if err != nil {
				logger.Warn("repair source lookup failed", "jobID", jobID, "sourceNodeID", val.SourceNodeID, "err", err.Error())
			} else {
				sourceAddr = sourceNode.Address
			}
		}

		targetAddr := ""
		if val.TargetNodeID != "" {
			targetNode, err := m.GetNode(val.TargetNodeID)
			if err != nil {
				logger.Warn("repair target lookup failed", "jobID", jobID, "targetNodeID", val.TargetNodeID, "err", err.Error())
			} else {
				targetAddr = targetNode.Address
			}
		}

		ins = append(ins, &pb.RepairInstruction{
			JobId:        val.JobID,
			ChunkId:      val.ChunkID,
			SourceNodeId: val.SourceNodeID,
			SourceAddr:   sourceAddr,
			TargetNodeId: val.TargetNodeID,
			TargetAddr:   targetAddr,
			DeleteSource: val.DeleteSource,
		})
	}

	return ins
}
