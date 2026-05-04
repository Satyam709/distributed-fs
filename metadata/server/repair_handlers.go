package server

import (
	"context"

	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
)

func (h *MetadataServiceHandler) ReportRepairResult(ctx context.Context, req *pb.ReportRepairResultRequest) (*pb.ReportRepairResultResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect(ctx)
	}
	if req.JobSucceed {
		err := h.deps.FSM.UpdateRepairJobStatus(h.deps.Raft, req.JobId, fsm.RepairStatusDone)
		if err != nil {
			return &pb.ReportRepairResultResponse{Success: false}, err
		}
	} else {
		err := h.deps.FSM.UpdateRepairJobStatus(h.deps.Raft, req.JobId, fsm.RepairStatusFailed)
		if err != nil {
			return &pb.ReportRepairResultResponse{Success: false}, err
		}
	}

	err := h.deps.Scheduler.OnJobComplete(req.NodeId, req.JobId, req.JobSucceed, req.Error)
	if err != nil {
		return &pb.ReportRepairResultResponse{Success: false}, err
	}
	return &pb.ReportRepairResultResponse{Success: true}, nil
}
