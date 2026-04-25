package replication_test

import (
	"context"
	"net"
	"testing"
	"time"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/replication"
	"github.com/satyam709/distributed-fs/storage/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeReplicationServer is an in-process peer that records received chunks.
type fakeReplicationServer struct {
	pb_storage.UnimplementedReplicationServiceServer
	received map[string][]byte
}

func (f *fakeReplicationServer) ReplicateChunk(
	stream grpc.BidiStreamingServer[pb_storage.ReplicateChunkRequest, pb_storage.ReplicateChunkResponse],
) error {
	var chunkId string
	var data []byte
	for {
		frame, err := stream.Recv()
		if err != nil {
			break
		}
		if frame.IsFirst {
			chunkId = frame.ChunkId
		}
		data = append(data, frame.Data...)
		if frame.IsLast {
			f.received[chunkId] = data
			_ = stream.Send(&pb_storage.ReplicateChunkResponse{
				Ok:      true,
				IsFinal: true,
				ChunkId: chunkId,
			})
			break
		}
		_ = stream.Send(&pb_storage.ReplicateChunkResponse{Ok: true})
	}
	return nil
}

// startFakePeer starts an in-process gRPC server and returns its address.
func startFakePeer(t *testing.T) (*fakeReplicationServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	fake := &fakeReplicationServer{received: make(map[string][]byte)}
	pb_storage.RegisterReplicationServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	return fake, lis.Addr().String()
}

// makeManager creates a ReplicationManager backed by a real DiskStore.
func makeManager(t *testing.T) (*replication.ReplicationManager, store.Store) {
	t.Helper()
	dir := t.TempDir()
	cs, err := store.NewChecksumIndexBoltDB[[]byte](store.ByteCodec{},
		store.WithDbPath[[]byte](dir),
	)
	require.NoError(t, err)
	require.NoError(t, cs.Open())
	t.Cleanup(cs.CleanUp)

	ds, err := store.NewDiskStore(
		store.WithRootDir(dir),
		store.WithTempDir(dir),
		store.WithSplitLevel(2),
		store.WithTotalSpace(64*1024*1024),
		store.WithChecksumStore(cs),
	)
	require.NoError(t, err)

	dialer := replication.NewPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	mgr := replication.NewReplicationManager(dialer, ds, &metaclient.MockMetaForNode{})
	mgr.Start()
	t.Cleanup(mgr.Stop)
	t.Cleanup(dialer.CloseAll)
	return mgr, ds
}

const mgr_chunkId = "deadbeef0123456789ab"

// writeChunk writes a chunk directly to the store for use as a replication source.
func writeChunk(t *testing.T, s store.Store, chunkId string, payload []byte) {
	t.Helper()
	require.NoError(t, s.Write(chunkId, payload))
}

// TestReplicationManager_ReplicateToNodes_AllSucceed fans out a chunk to two
// in-process peers and verifies both received the full payload.
func TestReplicationManager_ReplicateToNodes_AllSucceed(t *testing.T) {
	mgr, ds := makeManager(t)
	payload := []byte("hello replication world 1234567890abcdefgh") // >4 chars for shard
	writeChunk(t, ds, mgr_chunkId, payload)

	fake1, addr1 := startFakePeer(t)
	fake2, addr2 := startFakePeer(t)

	ctx := context.Background()
	err := mgr.ReplicateToNodes(ctx, mgr_chunkId, []string{addr1, addr2}, false)
	require.NoError(t, err)

	assert.Equal(t, payload, fake1.received[mgr_chunkId])
	assert.Equal(t, payload, fake2.received[mgr_chunkId])
}

// TestReplicationManager_ReplicateToNodes_NoTargets returns nil immediately.
func TestReplicationManager_ReplicateToNodes_NoTargets(t *testing.T) {
	mgr, ds := makeManager(t)
	writeChunk(t, ds, mgr_chunkId, []byte("payload 0123456789ab"))
	err := mgr.ReplicateToNodes(context.Background(), mgr_chunkId, nil, false)
	assert.NoError(t, err)
}

// TestReplicationManager_ReplicateToNodes_QuorumFailed verifies that when all
// targets are unreachable the manager returns an error.
func TestReplicationManager_ReplicateToNodes_QuorumFailed(t *testing.T) {
	mgr, ds := makeManager(t)
	writeChunk(t, ds, mgr_chunkId, []byte("payload 0123456789ab"))

	// Use non-listening addresses so all dials succeed but streams fail immediately.
	err := mgr.ReplicateToNodes(
		context.Background(),
		mgr_chunkId,
		[]string{"127.0.0.1:1", "127.0.0.1:2"},
		false,
	)
	assert.Error(t, err, "quorum should fail when all targets are unreachable")
}

// TestReplicationManager_EnqueueRepair_NonBlocking verifies that EnqueueRepair
// returns immediately (true) and a second call when queue is stuffed returns false.
func TestReplicationManager_EnqueueRepair_NonBlocking(t *testing.T) {
	dir := t.TempDir()
	cs, err := store.NewChecksumIndexBoltDB[[]byte](store.ByteCodec{},
		store.WithDbPath[[]byte](dir),
	)
	require.NoError(t, err)
	require.NoError(t, cs.Open())
	t.Cleanup(cs.CleanUp)

	ds, err := store.NewDiskStore(
		store.WithRootDir(dir),
		store.WithTempDir(dir),
		store.WithSplitLevel(2),
		store.WithTotalSpace(64*1024*1024),
		store.WithChecksumStore(cs),
	)
	require.NoError(t, err)

	// Create a manager but DON'T call Start() so workers don't drain the queue.
	dialer := replication.NewPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	mgr := replication.NewReplicationManager(dialer, ds, &metaclient.MockMetaForNode{})
	// Don't start — we want to fill the queue.

	// Fill 256 slots.
	for range 256 {
		ok := mgr.EnqueueRepair(replication.RepairJob{
			ChunkID: mgr_chunkId,
			Target:  "127.0.0.1:1",
		})
		assert.True(t, ok, "should accept job while queue has capacity")
	}
	// 257th should be dropped (queue full).
	ok := mgr.EnqueueRepair(replication.RepairJob{ChunkID: mgr_chunkId, Target: "127.0.0.1:1"})
	assert.False(t, ok, "should drop job when queue is full")
}

// TestReplicationManager_Stop_DrainsWorkers verifies Stop waits for goroutines
// and does not panic or deadlock.
func TestReplicationManager_Stop_DrainsWorkers(t *testing.T) {
	_, addr := startFakePeer(t)
	mgr, ds := makeManager(t)
	writeChunk(t, ds, mgr_chunkId, []byte("payload 0123456789ab"))
	_ = mgr.EnqueueRepair(replication.RepairJob{ChunkID: mgr_chunkId, Target: addr})

	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() timed out — possible deadlock")
	}
}
