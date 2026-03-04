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
// in a temp directory. Returns a connected client and a teardown func.
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

// buildFrames splits payload into data frames of at most frameSize bytes,
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

// chunkId valid for splitLevel=2 (≥4 chars).
const serverTestChunkId = "deadbeef0123456789ab"

// ---------------------------------------------------------------------------
// Tests
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

	// Chunk must be persisted and bit-for-bit correct.
	got, readErr := ds.Read(serverTestChunkId)
	require.NoError(t, readErr)
	assert.Equal(t, payload, got)
}

// TestPutChunk_SingleFrameIsLast exercises the single-frame path
// (first frame also has IsLast=true).
func TestPutChunk_SingleFrameIsLast(t *testing.T) {
	client, ds := newTestServer(t)

	payload := []byte("one-shot chunk payload")
	frames := buildFrames(serverTestChunkId, payload, len(payload)) // single frame
	resp, err := send(t, client, frames)
	require.NoError(t, err)
	assert.Equal(t, int32(200), resp.Response.Code)

	got, _ := ds.Read(serverTestChunkId)
	assert.Equal(t, payload, got)
}

// TestPutChunk_EmptyStream checks that a empty streams return error
func TestPutChunk_EmptyStream(t *testing.T) {
	client, _ := newTestServer(t)

	stream, err := client.PutChunk(context.Background())
	require.NoError(t, err)

	// havnt sent anything lets close it

	_, rpcErr := stream.CloseAndRecv()
	require.Error(t, rpcErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(rpcErr))
}

// TestPutChunk_ChecksumMismatch sends frames with a wrong final checksum and
// expects the server to reject with codes.Internal.
func TestPutChunk_ChecksumMismatch(t *testing.T) {
	client, _ := newTestServer(t)

	frames := buildFrames(serverTestChunkId, []byte("real data"), 1024)
	// Corrupt the checksum on the final frame.
	frames[len(frames)-1].Checksum = []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	_, rpcErr := send(t, client, frames)
	require.Error(t, rpcErr)
	assert.Equal(t, codes.Internal, status.Code(rpcErr))
}

// TestPutChunk_ResponseContainsChecksum verifies the echoed checksum in the
// response matches what the client sent.
func TestPutChunk_ResponseContainsChecksum(t *testing.T) {
	client, _ := newTestServer(t)

	payload := []byte("checksum echo test")
	h := sha256.Sum256(payload)
	expectedCS := h[:]

	frames := buildFrames(serverTestChunkId, payload, len(payload))
	resp, err := send(t, client, frames)
	require.NoError(t, err)

	assert.Equal(t, expectedCS, resp.Checksum)
}

// ---------------------------------------------------------------------------
// Unimplemented gRPC methods
// ---------------------------------------------------------------------------

func TestGetChunk_ReturnsUnimplemented(t *testing.T) {
	client, _ := newTestServer(t)
	stream, err := client.GetChunk(context.Background(), &pb_storage.GetChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)
	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.Unimplemented, status.Code(recvErr))
}

func TestDeleteChunk_ReturnsUnimplemented(t *testing.T) {
	client, _ := newTestServer(t)
	stream, err := client.DeleteChunk(context.Background(), &pb_storage.DeleteChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.NoError(t, err)
	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.Unimplemented, status.Code(recvErr))
}

func TestVerifyChunk_ReturnsUnimplemented(t *testing.T) {
	client, _ := newTestServer(t)
	_, err := client.VerifyChunk(context.Background(), &pb_storage.VerifyChunkRequest{
		ChunkId: serverTestChunkId,
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

// TestNewStorageServer_NilStore verifies the constructor returns an error when
// store is nil, so callers never get a server that will panic on the first RPC.
func TestNewStorageServer_NilStore(t *testing.T) {
	_, err := NewStorageServer(nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Store must not be nil")
}
