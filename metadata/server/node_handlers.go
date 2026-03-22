package server

import (
    "context"

    pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
)

func (h *MetadataServiceHandler) RegisterNode(ctx context.Context, req *pb.RegisterNodeRequest) (*pb.RegisterNodeResponse, error) {
    if !h.isLeader() {
        return nil, h.leaderRedirect();
    }
    return nil, nil;
}

func (h *MetadataServiceHandler) DeregisterNode(ctx context.Context, req *pb.DeregisterNodeRequest) (*pb.DeregisterNodeResponse, error) {
    if !h.isLeader() {
        return nil, h.leaderRedirect();
    }
    return nil, nil;
}

func (h *MetadataServiceHandler) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
    if !h.isLeader() {
        return nil, h.leaderRedirect();
    }
    // 1. NodeWatcher.UpdateLastSeen(nodeID, now) — in memory, not Raft
    // 2. Every 10th heartbeat: propose CmdUpdateNodeSpace
    // 3. Check RepairScheduler pending jobs for this node
    // 4. Return pending repair instructions
    return nil, nil;
}