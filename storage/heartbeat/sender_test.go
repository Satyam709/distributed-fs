package heartbeat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/replication"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

type mockNodeInfo struct {
	freeSpace  uint64
	nodeID     string
	chunkCount uint32
}

func (m *mockNodeInfo) GetFreeSpace() uint64  { return m.freeSpace }
func (m *mockNodeInfo) GetNodeID() string     { return m.nodeID }
func (m *mockNodeInfo) GetChunkCount() uint32 { return m.chunkCount }

type mockReplicator struct {
	mu          sync.Mutex
	enqueued    []replication.RepairJob
	shouldFail  bool
	failCounter int
}

func (m *mockReplicator) EnqueueRepair(job replication.RepairJob) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldFail && m.failCounter > 0 {
		m.failCounter--
		return false
	}
	m.enqueued = append(m.enqueued, job)
	return true
}

type mockMetadataClient struct {
	metaclient.StorageMetadataClientInterface
	heartbeatFunc  func(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error)
	reportFunc     func(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error)
	heartbeatCalls int
	reportCalls    int
}

func (m *mockMetadataClient) Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error) {
	m.heartbeatCalls++
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(ctx, in, opts...)
	}
	return &pb_meta.HeartbeatResponse{}, nil
}

func (m *mockMetadataClient) ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error) {
	m.reportCalls++
	if m.reportFunc != nil {
		return m.reportFunc(ctx, in, opts...)
	}
	return &pb_meta.ReportRepairResultResponse{}, nil
}

func TestNewHeartbeatSender(t *testing.T) {
	client := &mockMetadataClient{}
	ni := &mockNodeInfo{nodeID: "test-node", freeSpace: 1000, chunkCount: 10}
	rep := &mockReplicator{}

	sender := NewHeartbeatSender(5, client, ni, rep)
	assert.NotNil(t, sender)
	assert.Equal(t, uint64(1000), sender.nodeInfo.GetFreeSpace())
	assert.Equal(t, "test-node", sender.nodeInfo.GetNodeID())
}

func TestHeartbeatSender_StartAndStop(t *testing.T) {
	client := &mockMetadataClient{}
	ni := &mockNodeInfo{nodeID: "test-node", freeSpace: 1000, chunkCount: 10}
	rep := &mockReplicator{}

	sender := NewHeartbeatSender(time.Second, client, ni, rep)
	ctx, cancel := context.WithCancel(context.Background())

	sender.Start(ctx)
	time.Sleep(1200 * time.Millisecond)
	sender.StopAndWait(context.Background())
	cancel()

	assert.Greater(t, client.heartbeatCalls, 0)
}

func TestHeartbeatSender_StopBeforeStart(t *testing.T) {
	client := &mockMetadataClient{}
	ni := &mockNodeInfo{nodeID: "test-node"}
	rep := &mockReplicator{}

	sender := NewHeartbeatSender(time.Second, client, ni, rep)
	assert.NotPanics(t, func() {
		sender.StopAndWait(context.Background())
	})
}

func TestHeartbeatSender_StopIdempotent(t *testing.T) {
	client := &mockMetadataClient{}
	ni := &mockNodeInfo{nodeID: "test-node"}
	rep := &mockReplicator{}

	sender := NewHeartbeatSender(time.Second, client, ni, rep)
	ctx, cancel := context.WithCancel(context.Background())

	sender.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	sender.StopAndWait(context.Background())
	sender.StopAndWait(context.Background())
	cancel()
}

func TestHeartbeatSender_HeartbeatError(t *testing.T) {
	client := &mockMetadataClient{
		heartbeatFunc: func(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error) {
			return nil, errors.New("connection refused")
		},
	}
	ni := &mockNodeInfo{nodeID: "test-node"}
	rep := &mockReplicator{}

	sender := NewHeartbeatSender(time.Second, client, ni, rep)
	ctx, cancel := context.WithCancel(context.Background())

	sender.Start(ctx)
	time.Sleep(1200 * time.Millisecond)
	sender.StopAndWait(context.Background())
	cancel()

	assert.Greater(t, client.heartbeatCalls, 0)
	assert.Empty(t, rep.enqueued)
}

func TestHeartbeatSender_ProcessRepairJobs(t *testing.T) {
	client := &mockMetadataClient{
		heartbeatFunc: func(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error) {
			return &pb_meta.HeartbeatResponse{
				RepairJobs: []*pb_meta.RepairInstruction{
					{JobId: "job1", ChunkId: "chunk1", SourceAddr: "localhost:8001", TargetAddr: "localhost:8002"},
					{JobId: "job2", ChunkId: "chunk2", SourceAddr: "localhost:8001", TargetAddr: "localhost:8003"},
				},
			}, nil
		},
	}
	ni := &mockNodeInfo{nodeID: "test-node"}
	rep := &mockReplicator{}

	sender := NewHeartbeatSender(time.Second, client, ni, rep)
	ctx, cancel := context.WithCancel(context.Background())

	sender.Start(ctx)
	time.Sleep(1200 * time.Millisecond)
	sender.StopAndWait(context.Background())
	cancel()

	if len(rep.enqueued) > 0 {
		assert.Len(t, rep.enqueued, 2)
		assert.Equal(t, "job1", rep.enqueued[0].JobID)
		assert.Equal(t, "chunk1", rep.enqueued[0].ChunkID)
		assert.Equal(t, "localhost:8001", rep.enqueued[0].Source)
		assert.Equal(t, "localhost:8002", rep.enqueued[0].Target)
	}
}

func TestHeartbeatSender_RepairJobDropped(t *testing.T) {
	client := &mockMetadataClient{
		heartbeatFunc: func(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error) {
			return &pb_meta.HeartbeatResponse{
				RepairJobs: []*pb_meta.RepairInstruction{
					{JobId: "job1", ChunkId: "chunk1", SourceAddr: "localhost:8001", TargetAddr: "localhost:8002"},
				},
			}, nil
		},
	}
	ni := &mockNodeInfo{nodeID: "test-node"}
	rep := &mockReplicator{shouldFail: true, failCounter: 1}

	sender := NewHeartbeatSender(time.Second, client, ni, rep)
	ctx, cancel := context.WithCancel(context.Background())

	sender.Start(ctx)
	time.Sleep(1200 * time.Millisecond)
	sender.StopAndWait(context.Background())
	cancel()

	assert.Len(t, rep.enqueued, 0)
}

func TestHeartbeatSender_NodeInfoInterface(t *testing.T) {
	var _ NodeInfo = &mockNodeInfo{}
}
