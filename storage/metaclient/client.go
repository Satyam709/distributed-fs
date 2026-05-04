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

// StorageMetadataClientInterface is the subset of MetadataServiceClient
// used by storage nodes (registration, heartbeat, repair reporting, chunk
// commit). Both the production LeaderAwareClient-backed implementation
// and the in-memory MockMetaForNode satisfy this interface.
type StorageMetadataClientInterface interface {
	RegisterNode(ctx context.Context, in *pb_meta.RegisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.RegisterNodeResponse, error)
	DeregisterNode(ctx context.Context, in *pb_meta.DeregisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.DeregisterNodeResponse, error)
	Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error)
	ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error)
	CommitChunk(ctx context.Context, in *pb_meta.CommitChunkRequest, opts ...grpc.CallOption) (*pb_meta.CommitChunkResponse, error)
	Close() error
}

// StorageMetadataClient wraps a LeaderAwareClient that transparently
// handles metadata leader redirection, address rotation, and retry.
// The generated MetadataServiceClient stub is created on top of the
// LeaderAwareClient and delegates all RPC routing to it.
type StorageMetadataClient struct {
	lc         *leaderclient.LeaderAwareClient
	metaClient pb_meta.MetadataServiceClient
	timeout    time.Duration
}

// NewMetadataClient creates a metadata client that connects to the first
// reachable address from seedAddrs. Redirects are followed transparently;
// connectivity failures are retried according to the retry policy.
func NewMetadataClient(seedAddrs []string, rp retry.Policy, timeout time.Duration) (*StorageMetadataClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lc, err := leaderclient.New(ctx, seedAddrs, rp)
	if err != nil {
		return nil, fmt.Errorf("new metadata client: %w", err)
	}
	return &StorageMetadataClient{
		lc:         lc,
		metaClient: pb_meta.NewMetadataServiceClient(lc),
		timeout:    timeout,
	}, nil
}

// RegisterNode registers or re-registers a storage node with the metadata
// cluster. Each call acquires its own context with the configured timeout.
func (m *StorageMetadataClient) RegisterNode(ctx context.Context, in *pb_meta.RegisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.RegisterNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.metaClient.RegisterNode(ctx, in, opts...)
}

// DeregisterNode marks a storage node as draining for graceful shutdown.
func (m *StorageMetadataClient) DeregisterNode(ctx context.Context, in *pb_meta.DeregisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.DeregisterNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.metaClient.DeregisterNode(ctx, in, opts...)
}

// Heartbeat sends a periodic liveness heartbeat and returns any piggybacked
// repair instructions from the metadata service.
func (m *StorageMetadataClient) Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.metaClient.Heartbeat(ctx, in, opts...)
}

// ReportRepairResult notifies the metadata service that a repair job
// completed (or failed).
func (m *StorageMetadataClient) ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.metaClient.ReportRepairResult(ctx, in, opts...)
}

// CommitChunk confirms that a chunk was stored and replicated successfully.
func (m *StorageMetadataClient) CommitChunk(ctx context.Context, in *pb_meta.CommitChunkRequest, opts ...grpc.CallOption) (*pb_meta.CommitChunkResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	return m.metaClient.CommitChunk(ctx, in, opts...)
}

func (m *StorageMetadataClient) Close() error {
	return m.lc.Close()
}

// MockMetaForNode is an in-memory no-op implementation used in tests.
// All methods return a zero-value success response without making any
// network calls.
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

func (m *MockMetaForNode) Close() error {
	return nil
}
