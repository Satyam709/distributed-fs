package server_test

import (
	"context"
	"net"
	"testing"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/storage/server"
	"github.com/satyam709/distributed-fs/storage/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const replBufSize = 1 << 20

// newReplTestServer spins up an in-process gRPC server hosting the ReplicationService.
// Returns a connected client plus the underlying store.
func newReplTestServer(t *testing.T) (pb_storage.ReplicationServiceClient, store.Store) {
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

	lis := bufconn.Listen(replBufSize)
	srv := grpc.NewServer()
	rs, err := server.NewReplicationServer(ds, nil)
	require.NoError(t, err)
	pb_storage.RegisterReplicationServiceServer(srv, rs)

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
	return pb_storage.NewReplicationServiceClient(conn), ds
}

const replChunkId = "deadbeef0123456789ab"

// sendReplicateFrames opens a ReplicateChunk bidi stream and sends frames.
// Returns the final response (IsFinal=true).
func sendReplicateFrames(t *testing.T, client pb_storage.ReplicationServiceClient,
	chunkId string, payload []byte, frameSize int) (*pb_storage.ReplicateChunkResponse, error) {
	t.Helper()
	stream, err := client.ReplicateChunk(context.Background())
	require.NoError(t, err)

	isFirst := true
	for start := 0; start < len(payload); start += frameSize {
		end := min(start+frameSize, len(payload))
		isLast := end == len(payload)
		req := &pb_storage.ReplicateChunkRequest{
			ChunkId: chunkId,
			Data:    payload[start:end],
			IsFirst: isFirst,
			IsLast:  isLast,
		}
		isFirst = false
		require.NoError(t, stream.Send(req))

		// Receive ack or final response.
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			return nil, recvErr
		}
		if resp.IsFinal {
			return resp, nil
		}
	}

	// Drain final response if not yet received (single-frame path).
	_ = stream.CloseSend()
	return stream.Recv()
}

// TestReplicateChunk_HappyPath streams a chunk in multiple frames and verifies
// it lands in the store and the final response carries IsFinal=true.
func TestReplicateChunk_HappyPath(t *testing.T) {
	client, ds := newReplTestServer(t)

	payload := make([]byte, 3*1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	resp, err := sendReplicateFrames(t, client, replChunkId, payload, 1024)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Ok)
	assert.True(t, resp.IsFinal)
	assert.Equal(t, replChunkId, resp.ChunkId)

	// Chunk must be visible in the store.
	got, readErr := ds.Read(replChunkId)
	require.NoError(t, readErr)
	assert.Equal(t, payload, got)
}

// TestReplicateChunk_SingleFrame sends a single is_last frame.
func TestReplicateChunk_SingleFrame(t *testing.T) {
	client, ds := newReplTestServer(t)

	payload := []byte("single frame data abcdefgh0123456789")
	resp, err := sendReplicateFrames(t, client, replChunkId, payload, len(payload))
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.IsFinal)

	got, _ := ds.Read(replChunkId)
	assert.Equal(t, payload, got)
}

// TestReplicateChunk_EmptyStream closes the stream immediately; expects an error.
func TestReplicateChunk_EmptyStream(t *testing.T) {
	client, _ := newReplTestServer(t)

	stream, err := client.ReplicateChunk(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.CloseSend())

	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(recvErr))
}

// TestReplicateChunk_MissingIsFirst sends a frame without is_first; expects error.
func TestReplicateChunk_MissingIsFirst(t *testing.T) {
	client, _ := newReplTestServer(t)

	stream, err := client.ReplicateChunk(context.Background())
	require.NoError(t, err)

	// Send a frame without is_first — handler expects first frame to have is_first=true.
	require.NoError(t, stream.Send(&pb_storage.ReplicateChunkRequest{
		ChunkId: replChunkId,
		Data:    []byte("data"),
		IsFirst: false,
		IsLast:  true,
	}))

	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(recvErr))
}

// TestReplicateChunk_ChunkIdMismatch sends frames with mismatched chunk_id; expects error.
func TestReplicateChunk_ChunkIdMismatch(t *testing.T) {
	client, _ := newReplTestServer(t)

	stream, err := client.ReplicateChunk(context.Background())
	require.NoError(t, err)

	require.NoError(t, stream.Send(&pb_storage.ReplicateChunkRequest{
		ChunkId: replChunkId,
		Data:    []byte("frame1 0123456789ab"),
		IsFirst: true,
	}))
	ack, recvErr := stream.Recv()
	require.NoError(t, recvErr)
	require.True(t, ack.Ok)

	// Second frame with different chunk_id.
	require.NoError(t, stream.Send(&pb_storage.ReplicateChunkRequest{
		ChunkId: "mismatch_chunk_abcd",
		Data:    []byte("frame2"),
		IsLast:  true,
	}))
	_, recvErr = stream.Recv()
	require.Error(t, recvErr)
	assert.Equal(t, codes.InvalidArgument, status.Code(recvErr))
}

// TestNewReplicationServer_NilStore verifies that a nil store returns an error.
func TestNewReplicationServer_NilStore(t *testing.T) {
	_, err := server.NewReplicationServer(nil, nil)
	require.Error(t, err)
}
