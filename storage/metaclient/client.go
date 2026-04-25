package metaclient

import (
	"context"
	"fmt"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A sub interface of pb_meta.NewMetadataServiceClient, only comprises of operations allowed from storage node
type StorageMetadataClientInterface interface {
	RegisterNode(ctx context.Context, in *pb_meta.RegisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.RegisterNodeResponse, error)
	DeregisterNode(ctx context.Context, in *pb_meta.DeregisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.DeregisterNodeResponse, error)
	Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error)
	ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error)
	CommitChunk(ctx context.Context, in *pb_meta.CommitChunkRequest, opts ...grpc.CallOption) (*pb_meta.CommitChunkResponse, error)
}

type StorageMetadataClient struct {
	StorageMetadataClientInterface
}

func NewMetadataClient(addr string) (*StorageMetadataClient, error) {
	grpcClient, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("new metadata client: %w", err)
	}
	return &StorageMetadataClient{
		pb_meta.NewMetadataServiceClient(grpcClient),
	}, nil
}

// MockMetaForNode A nil implementation Only for testing purposes
type MockMetaForNode struct {
	StorageMetadataClientInterface
}

func (m *MockMetaForNode) RegisterNode(ctx context.Context, in *pb_meta.RegisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.RegisterNodeResponse, error) {
	return &pb_meta.RegisterNodeResponse{}, nil
}

func (m *MockMetaForNode) DeregisterNode(ctx context.Context, in *pb_meta.DeregisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.DeregisterNodeResponse, error) {
	return &pb_meta.DeregisterNodeResponse{}, nil
}

func (m *MockMetaForNode) Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error) {
	return &pb_meta.HeartbeatResponse{}, nil
}

func (m *MockMetaForNode) ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error) {
	return &pb_meta.ReportRepairResultResponse{}, nil
}

func (m *MockMetaForNode) CommitChunk(ctx context.Context, in *pb_meta.CommitChunkRequest, opts ...grpc.CallOption) (*pb_meta.CommitChunkResponse, error) {
	return &pb_meta.CommitChunkResponse{}, nil
}
