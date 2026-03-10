package server

import (
	"context"
	"crypto/sha256"
	"net"
	"testing"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/storage/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1 MiB

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

// newTestServer spins up an in-process gRPC server backed by a real DiskStore
// in a temp directory. Returns a connected client and the underlying store.
func newTestServer(t *testing.T) (pb_storage.StorageServiceClient, store.Store) {
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

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	ss, err := NewStorageServer(ds, nil)
	require.NoError(t, err)
	pb_storage.RegisterStorageServiceServer(srv, ss)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return pb_storage.NewStorageServiceClient(conn), ds
}

// buildFrames splits payload into data frames of at most frameSize bytes.
func buildFrames(chunkId string, payload []byte, frameSize int) []*pb_storage.PutChunkRequest {
	cs := sha256.New()
	var frames []*pb_storage.PutChunkRequest
	for start := 0; start < len(payload); start += frameSize {
		end := min(start+frameSize, len(payload))
		data := payload[start:end]
		cs.Write(data)
		frames = append(frames, &pb_storage.PutChunkRequest{
			ChunkId:  chunkId,
			Data:     data,
			IsLast:   end == len(payload),
			Checksum: cs.Sum(nil),
		})
	}
	return frames
}

// send streams all frames to the server and returns the response.
func send(t *testing.T, client pb_storage.StorageServiceClient, frames []*pb_storage.PutChunkRequest) (*pb_storage.PutChunkResponse, error) {
	t.Helper()
	stream, err := client.PutChunk(context.Background())
	require.NoError(t, err)
	for _, f := range frames {
		require.NoError(t, stream.Send(f))
	}
	return stream.CloseAndRecv()
}

// putChunk uploads a payload in a single frame and asserts success.
func putChunk(t *testing.T, client pb_storage.StorageServiceClient, chunkId string, payload []byte) {
	t.Helper()
	frames := buildFrames(chunkId, payload, len(payload))
	resp, err := send(t, client, frames)
	require.NoError(t, err)
	require.Equal(t, int32(200), resp.Response.Code)
}

// chunkId valid for splitLevel=2 (≥4 chars).
const serverTestChunkId = "deadbeef0123456789ab"

// ---------------------------------------------------------------------------
// PutChunk tests
// ---------------------------------------------------------------------------

// TestPutChunk_HappyPath sends a multi-frame payload and checks that
// the chunk lands in the store with a 200 response.
func TestPutChunk_HappyPath(t *testing.T) {
	client, ds := newTestServer(t)

	payload := make([]byte, 3*1024) // 3 KiB
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	frames := buildFrames(serverTestChunkId, payload, 1024) // 3 frames of 1 KiB each
	resp, err := send(t, client, frames)
	require.NoError(t, err)

	assert.Equal(t, int32(200), resp.Response.Code)
	assert.Equal(t, serverTestChunkId, resp.ChunkId)

	got, readErr := ds.Read(serverTestChunkId)
	require.NoError(t, readErr)
	assert.Equal(t, payload, got)
}

func TestPutChunk_ChunkIdChangedMidStream(t *testing.T) {
	client, ds := newTestServer(t)

	payload := make([]byte, 3*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	frames := buildFrames(serverTestChunkId, payload, 1024)
	frames[1].ChunkId = "mismatch"

	resp, err := send(t, client, frames)
	assert.Error(t, err)
	assert.Nil(t, resp)
	_, readErr := ds.Read(serverTestChunkId)
	assert.Error(t, readErr)
}

// TestPutChunk_SingleFrameIsLast exercises the single-frame path.
func TestPutChunk_SingleFrameIsLast(t *testing.T) {
	client, ds := newTestServer(t)

	payload := []byte("one-shot chunk payload")
	frames := buildFrames(serverTestChunkId, payload, len(payload))
	resp, err := send(t, client, frames)
	require.NoError(t, err)
	assert.Equal(t, int32(200), resp.Response.Code)

	got, _ := ds.Read(serverTestChunkId)
	assert.Equal(t, payload, got)
}

// TestPutChunk_EmptyStream checks that empty streams return an error.
func TestPutChunk_EmptyStream(t *testing.T) {
	client, _ := newTestServer(t)

	stream, err := client.PutChunk(context.Background())
	require.NoError(t, err)

	_, rpcErr := stream.CloseAndRecv()
	require.Error(t, rpcErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(rpcErr))
}

// TestPutChunk_ChecksumMismatch expects codes.Internal for a bad checksum.
func TestPutChunk_ChecksumMismatch(t *testing.T) {
	client, _ := newTestServer(t)

	frames := buildFrames(serverTestChunkId, []byte("real data"), 1024)
	frames[len(frames)-1].Checksum = []byte("aaaaaaaaaa")

	_, rpcErr := send(t, client, frames)
	require.Error(t, rpcErr)
	assert.Equal(t, codes.Internal, status.Code(rpcErr))
}

// TestPutChunk_ResponseContainsChecksum verifies the echoed checksum in the response.
func TestPutChunk_ResponseContainsChecksum(t *testing.T) {
	client, _ := newTestServer(t)

	payload := []byte("checksum echo test")
	h := sha256.Sum256(payload)

	frames := buildFrames(serverTestChunkId, payload, len(payload))
	resp, err := send(t, client, frames)
	require.NoError(t, err)
	assert.Equal(t, h[:], resp.Checksum)
}

// ---------------------------------------------------------------------------
// GetChunk tests
// ---------------------------------------------------------------------------

// TestGetChunk_HappyPath uploads a chunk then streams it back,
// reassembling the payload and checking byte equality.
func TestGetChunk_HappyPath(t *testing.T) {
	client, _ := newTestServer(t)

	payload := make([]byte, 3*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	putChunk(t, client, serverTestChunkId, payload)

	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	var got []byte
	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		got = append(got, frame.Data...)
		if frame.IsLast {
			_, _ = stream.Recv() // drain EOF
			break
		}
	}
	assert.Equal(t, payload, got)
}

// TestGetChunk_ChunkIdInResponse ensures every frame carries the correct chunk_id.
func TestGetChunk_ChunkIdInResponse(t *testing.T) {
	client, _ := newTestServer(t)

	putChunk(t, client, serverTestChunkId, []byte("id echo test"))

	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		assert.Equal(t, serverTestChunkId, frame.ChunkId)
	}
}

// TestGetChunk_ChecksumPerFrame verifies per-frame checksum = SHA-256(frame.Data).
func TestGetChunk_ChecksumPerFrame(t *testing.T) {
	client, _ := newTestServer(t)

	payload := make([]byte, 3*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	putChunk(t, client, serverTestChunkId, payload)

	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		expected := sha256.Sum256(frame.Data)
		assert.Equal(t, expected[:], frame.Checksum,
			"per-frame checksum mismatch for frame len=%d", len(frame.Data))
	}
}

// TestGetChunk_LastFrameMarked verifies exactly one frame has IsLast=true.
func TestGetChunk_LastFrameMarked(t *testing.T) {
	client, _ := newTestServer(t)

	putChunk(t, client, serverTestChunkId, make([]byte, 3*1024))

	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	var lastCount int
	var seenLast bool
	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		if seenLast {
			t.Error("received frame after IsLast=true")
		}
		if frame.IsLast {
			lastCount++
			seenLast = true
		}
	}
	assert.Equal(t, 1, lastCount, "expected exactly one frame with IsLast=true")
}

// TestGetChunk_UnknownChunk expects NotFound when the chunk does not exist.
func TestGetChunk_UnknownChunk(t *testing.T) {
	client, _ := newTestServer(t)

	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.NotFound, status.Code(recvErr))
}

// TestGetChunk_EmptyChunkId expects InvalidArgument for an empty chunk_id.
func TestGetChunk_EmptyChunkId(t *testing.T) {
	client, _ := newTestServer(t)

	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: "",
	})
	require.NoError(t, err)

	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(recvErr))
}

// TestGetChunk_MultiFrameReassembly uploads a 200 KiB payload (spans many frames)
// and verifies correct reassembly on the client side.
func TestGetChunk_MultiFrameReassembly(t *testing.T) {
	client, _ := newTestServer(t)

	payload := make([]byte, 200*1024)
	for i := range payload {
		payload[i] = byte(i % 199)
	}
	putChunk(t, client, serverTestChunkId, payload)

	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	var reassembled []byte
	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		reassembled = append(reassembled, frame.Data...)
		if frame.IsLast {
			_, _ = stream.Recv() // drain EOF
			break
		}
	}
	assert.Equal(t, payload, reassembled)
}

// ---------------------------------------------------------------------------
// DeleteChunk tests
// ---------------------------------------------------------------------------

// TestDeleteChunk_HappyPath uploads a chunk then deletes it and verifies
// it is gone from the store.
func TestDeleteChunk_HappyPath(t *testing.T) {
	client, ds := newTestServer(t)

	putChunk(t, client, serverTestChunkId, []byte("delete me"))
	require.True(t, ds.Exists(serverTestChunkId))

	stream, err := client.DeleteChunk(context.Background(), &pb_storage.DeleteChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	resp, recvErr := stream.Recv()
	require.NoError(t, recvErr)
	assert.True(t, resp.Success)
	assert.Equal(t, int32(200), resp.Response.Code)

	assert.False(t, ds.Exists(serverTestChunkId), "chunk should be absent after delete")
}

// TestDeleteChunk_Idempotent checks that deleting a non-existent chunk
// succeeds (idempotent — not an error if already gone, per spec).
func TestDeleteChunk_Idempotent(t *testing.T) {
	client, _ := newTestServer(t)

	stream, err := client.DeleteChunk(context.Background(), &pb_storage.DeleteChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)

	resp, recvErr := stream.Recv()
	require.NoError(t, recvErr)
	assert.True(t, resp.Success)
}

// TestDeleteChunk_EmptyChunkId expects InvalidArgument for an empty chunk_id.
func TestDeleteChunk_EmptyChunkId(t *testing.T) {
	client, _ := newTestServer(t)

	stream, err := client.DeleteChunk(context.Background(), &pb_storage.DeleteChunkRequest{
		ChunkId: "",
	})
	require.NoError(t, err)

	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(recvErr))
}

// ---------------------------------------------------------------------------
// VerifyChunk tests
// ---------------------------------------------------------------------------

// TestVerifyChunk_HappyPath uploads a chunk and verifies it without supplying
// an explicit expected checksum — server checks against its stored checksum.
func TestVerifyChunk_HappyPath(t *testing.T) {
	client, _ := newTestServer(t)

	putChunk(t, client, serverTestChunkId, []byte("verify me"))

	resp, err := client.VerifyChunk(context.Background(), &pb_storage.VerifyChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)
	assert.True(t, resp.IsValid)
}

// TestVerifyChunk_WithCorrectChecksum supplies the correct expected checksum.
func TestVerifyChunk_WithCorrectChecksum(t *testing.T) {
	client, _ := newTestServer(t)

	payload := []byte("verify with correct checksum")
	h := sha256.Sum256(payload)
	putChunk(t, client, serverTestChunkId, payload)

	resp, err := client.VerifyChunk(context.Background(), &pb_storage.VerifyChunkRequest{
		ChunkId:  serverTestChunkId,
		Checksum: h[:],
	})
	require.NoError(t, err)
	assert.True(t, resp.IsValid)
}

// TestVerifyChunk_WithWrongChecksum supplies an incorrect expected checksum;
// expects IsValid=false (not an RPC error — corruption is a data-level result).
func TestVerifyChunk_WithWrongChecksum(t *testing.T) {
	client, _ := newTestServer(t)

	putChunk(t, client, serverTestChunkId, []byte("verify with wrong checksum"))

	resp, err := client.VerifyChunk(context.Background(), &pb_storage.VerifyChunkRequest{
		ChunkId:  serverTestChunkId,
		Checksum: make([]byte, 32), // all-zero — will not match
	})
	require.NoError(t, err)
	assert.False(t, resp.IsValid)
}

// TestVerifyChunk_UnknownChunk expects NotFound when the chunk does not exist.
func TestVerifyChunk_UnknownChunk(t *testing.T) {
	client, _ := newTestServer(t)

	_, err := client.VerifyChunk(context.Background(), &pb_storage.VerifyChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// TestVerifyChunk_EmptyChunkId expects InvalidArgument for an empty chunk_id.
func TestVerifyChunk_EmptyChunkId(t *testing.T) {
	client, _ := newTestServer(t)

	_, err := client.VerifyChunk(context.Background(), &pb_storage.VerifyChunkRequest{
		ChunkId: "",
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

// TestNewStorageServer_NilStore verifies that passing a nil store returns an error.
func TestNewStorageServer_NilStore(t *testing.T) {
	_, err := NewStorageServer(nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Store must not be nil")
}

// ---------------------------------------------------------------------------
// Replication fanout tests
// ---------------------------------------------------------------------------

// fakeReplicator records the last ReplicateToNodes call for assertion.
type fakeReplicator struct {
	calledWith struct {
		chunkId string
		targets []string
	}
	callCount int
}

func (f *fakeReplicator) ReplicateToNodes(_ context.Context, chunkId string, targets []string, _ bool) error {
	f.callCount++
	f.calledWith.chunkId = chunkId
	f.calledWith.targets = append([]string(nil), targets...)
	return nil
}

// newTestServerWithReplicator builds an in-process server with an injected replicator.
func newTestServerWithReplicator(t *testing.T, r Replicator) pb_storage.StorageServiceClient {
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

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	ss, err := NewStorageServer(ds, nil, r)
	require.NoError(t, err)
	pb_storage.RegisterStorageServiceServer(srv, ss)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return pb_storage.NewStorageServiceClient(conn)
}

// TestPutChunk_TriggersReplication verifies that PutChunk calls the Replicator
// with the correct chunkId and target addresses extracted from replicate_to.
func TestPutChunk_TriggersReplication(t *testing.T) {
	replic := &fakeReplicator{}
	client := newTestServerWithReplicator(t, replic)

	payload := []byte("replication trigger payload")
	frames := buildFrames(serverTestChunkId, payload, len(payload))

	// Inject replicate_to addresses on the first frame.
	frames[0].ReplicateTo = []*pb_storage.NodeInfo{
		{Address: "10.0.0.1:4001"},
		{Address: "10.0.0.2:4001"},
	}
	_, err := send(t, client, frames)
	// Replication to unreachable addresses will fail, but PutChunk itself
	// should still return 200 (partial failure is swallowed per design doc).
	require.NoError(t, err)

	assert.Equal(t, 1, replic.callCount, "ReplicateToNodes must be called once")
	assert.Equal(t, serverTestChunkId, replic.calledWith.chunkId)
	assert.Equal(t, []string{"10.0.0.1:4001", "10.0.0.2:4001"}, replic.calledWith.targets)
}

// TestPutChunk_NoFanoutWithoutReplicator verifies that PutChunk works normally
// when no Replicator is wired in (nil).
func TestPutChunk_NoFanoutWithoutReplicator(t *testing.T) {
	client, _ := newTestServer(t) // no replicator

	payload := []byte("no fanout payload")
	frames := buildFrames(serverTestChunkId, payload, len(payload))
	frames[0].ReplicateTo = []*pb_storage.NodeInfo{{Address: "10.0.0.1:4001"}}

	resp, err := send(t, client, frames)
	require.NoError(t, err)
	assert.Equal(t, int32(200), resp.Response.Code)
}
