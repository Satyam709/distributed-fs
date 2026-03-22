package server

import (
    "context"

    pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
)

func (h *MetadataServiceHandler) CommitChunk(ctx context.Context, req *pb.CommitChunkRequest) (*pb.CommitChunkResponse, error) {
    if !h.isLeader() {
        return nil, h.leaderRedirect();
    }
    return nil, nil;
}

func (h *MetadataServiceHandler) GetChunkLocations(ctx context.Context, req *pb.GetChunkLocationsRequest) (*pb.GetChunkLocationsResponse, error) {
    if !h.isLeader() {
        return nil, h.leaderRedirect();
    }
    return nil, nil;
}

func (h *MetadataServiceHandler) ReportCorruption(ctx context.Context, req *pb.ReportCorruptionRequest) (*pb.ReportCorruptionResponse, error) {
    if !h.isLeader() {
        return nil, h.leaderRedirect();
    }
    return nil, nil;
}