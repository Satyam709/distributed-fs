package metaclient

import (
	"context"
	"fmt"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/leaderclient"
	"github.com/satyam709/distributed-fs/internal/retry"
	"google.golang.org/grpc"
)

type StorageMetadataClientInterface interface {
	RegisterNode(ctx context.Context, in *pb_meta.RegisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.RegisterNodeResponse, error)
	DeregisterNode(ctx context.Context, in *pb_meta.DeregisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.DeregisterNodeResponse, error)
	Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error)
	ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error)
	CommitChunk(ctx context.Context, in *pb_meta.CommitChunkRequest, opts ...grpc.CallOption) (*pb_meta.CommitChunkResponse, error)
}

const metadataServicePath = "/proto.metadata.v1.MetadataService/"

type StorageMetadataClient struct {
	client  *leaderclient.LeaderAwareClient
	timeout time.Duration
}

func NewMetadataClient(seedAddrs []string, rp retry.Policy, timeout time.Duration) (*StorageMetadataClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lc, err := leaderclient.New(ctx, seedAddrs, rp)
	if err != nil {
		return nil, fmt.Errorf("new metadata client: %w", err)
	}
	return &StorageMetadataClient{client: lc, timeout: timeout}, nil
}

func (m *StorageMetadataClient) RegisterNode(ctx context.Context, in *pb_meta.RegisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.RegisterNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	var resp pb_meta.RegisterNodeResponse
	if err := m.client.Invoke(ctx, metadataServicePath+"RegisterNode", in, &resp, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (m *StorageMetadataClient) DeregisterNode(ctx context.Context, in *pb_meta.DeregisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.DeregisterNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	var resp pb_meta.DeregisterNodeResponse
	if err := m.client.Invoke(ctx, metadataServicePath+"DeregisterNode", in, &resp, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (m *StorageMetadataClient) Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	var resp pb_meta.HeartbeatResponse
	if err := m.client.Invoke(ctx, metadataServicePath+"Heartbeat", in, &resp, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (m *StorageMetadataClient) ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	var resp pb_meta.ReportRepairResultResponse
	if err := m.client.Invoke(ctx, metadataServicePath+"ReportRepairResult", in, &resp, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (m *StorageMetadataClient) CommitChunk(ctx context.Context, in *pb_meta.CommitChunkRequest, opts ...grpc.CallOption) (*pb_meta.CommitChunkResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	var resp pb_meta.CommitChunkResponse
	if err := m.client.Invoke(ctx, metadataServicePath+"CommitChunk", in, &resp, opts...); err != nil {
		return nil, err
	}
	return &resp, nil
}

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
