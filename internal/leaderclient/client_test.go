package leaderclient

import (
	"context"
	"net"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testMethod = "/proto.metadata.v1.MetadataService/CreateFile"

// controlledHandler lets each test control what the server returns.
type controlledHandler struct {
	pb_meta.UnimplementedMetadataServiceServer
	fn func(context.Context, *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error)
}

func (h *controlledHandler) CreateFile(ctx context.Context, req *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error) {
	return h.fn(ctx, req)
}

// startTestServer starts a gRPC server on a random port and returns its address.
func startTestServer(t *testing.T, fn func(context.Context, *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	pb_meta.RegisterMetadataServiceServer(srv, &controlledHandler{fn: fn})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// redirectHandler returns FailedPrecondition and sets the x-leader-grpc-addr trailer.
func redirectHandler(leaderAddr string) func(context.Context, *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error) {
	return func(ctx context.Context, req *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error) {
		_ = grpc.SetTrailer(ctx, metadata.Pairs("x-leader-grpc-addr", leaderAddr))
		return nil, status.Error(codes.FailedPrecondition, "not leader")
	}
}

var successHandler = func(ctx context.Context, req *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error) {
	return &pb_meta.CreateFileResponse{FileId: "file-1"}, nil
}

var notFoundHandler = func(ctx context.Context, req *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error) {
	return nil, status.Error(codes.NotFound, "file not found")
}

func makeRetryPolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts: 3,
		Base:        10 * time.Millisecond,
		Max:         100 * time.Millisecond,
		Multiplier:  2.0,
	}
}

func TestInvoke_Success(t *testing.T) {
	addr := startTestServer(t, successHandler)
	lc, err := New(context.Background(), []string{addr}, makeRetryPolicy())
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	req := &pb_meta.CreateFileRequest{}
	var resp pb_meta.CreateFileResponse
	err = lc.Invoke(context.Background(), testMethod, req, &resp)
	assert.NoError(t, err)
	assert.Equal(t, "file-1", resp.GetFileId())
}

func TestInvoke_LeaderRedirect_FollowsRedirect(t *testing.T) {
	leaderAddr := startTestServer(t, successHandler)
	followerAddr := startTestServer(t, redirectHandler(leaderAddr))

	lc, err := New(context.Background(), []string{followerAddr}, makeRetryPolicy())
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	req := &pb_meta.CreateFileRequest{}
	var resp pb_meta.CreateFileResponse
	err = lc.Invoke(context.Background(), testMethod, req, &resp)
	assert.NoError(t, err)
	assert.Equal(t, "file-1", resp.GetFileId())
}

func TestInvoke_RetryOnUnavailable(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr := lis.Addr().String()

	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	srv.Stop()
	_ = lis.Close()

	liveAddr := startTestServer(t, successHandler)

	lc, err := New(context.Background(), []string{deadAddr, liveAddr}, makeRetryPolicy())
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	req := &pb_meta.CreateFileRequest{}
	var resp pb_meta.CreateFileResponse
	err = lc.Invoke(context.Background(), testMethod, req, &resp)
	assert.NoError(t, err)
	assert.Equal(t, "file-1", resp.GetFileId())
}

func TestInvoke_NonRetryableError_ReturnedImmediately(t *testing.T) {
	addr := startTestServer(t, notFoundHandler)
	lc, err := New(context.Background(), []string{addr}, retry.Policy{
		MaxAttempts: 3,
		Base:        50 * time.Millisecond,
		Max:         200 * time.Millisecond,
		Multiplier:  2.0,
	})
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	start := time.Now()
	req := &pb_meta.CreateFileRequest{}
	var resp pb_meta.CreateFileResponse
	err = lc.Invoke(context.Background(), testMethod, req, &resp)
	elapsed := time.Since(start)

	assert.Error(t, err)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Less(t, elapsed, 50*time.Millisecond, "non-retryable errors must return immediately")
}

func TestInvoke_ContextCancelled(t *testing.T) {
	addr := startTestServer(t, func(ctx context.Context, req *pb_meta.CreateFileRequest) (*pb_meta.CreateFileResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	})
	lc, err := New(context.Background(), []string{addr}, retry.Policy{
		MaxAttempts: 5,
		Base:        50 * time.Millisecond,
		Max:         500 * time.Millisecond,
		Multiplier:  2.0,
	})
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := &pb_meta.CreateFileRequest{}
	var resp pb_meta.CreateFileResponse
	err = lc.Invoke(ctx, testMethod, req, &resp)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestInvoke_AllSeedsUnreachable_ReturnsError(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr1 := lis.Addr().String()

	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	srv.Stop()
	_ = lis.Close()

	lis2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	deadAddr2 := lis2.Addr().String()

	srv2 := grpc.NewServer()
	go func() { _ = srv2.Serve(lis2) }()
	srv2.Stop()
	_ = lis2.Close()

	rp := retry.Policy{
		MaxAttempts: 2,
		Base:        10 * time.Millisecond,
		Max:         50 * time.Millisecond,
		Multiplier:  2.0,
	}

	lc, err := New(context.Background(), []string{deadAddr1, deadAddr2}, rp)
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	req := &pb_meta.CreateFileRequest{}
	var resp pb_meta.CreateFileResponse
	err = lc.Invoke(context.Background(), testMethod, req, &resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed after")
}

func TestInvoke_RedirectThenFallback_Retries(t *testing.T) {
	leaderAddr := startTestServer(t, successHandler)
	followerAddr := startTestServer(t, redirectHandler(leaderAddr))

	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	deadAddr := lis.Addr().String()
	srv := grpc.NewServer()
	go func() { _ = srv.Serve(lis) }()
	srv.Stop()
	_ = lis.Close()

	lc, err := New(context.Background(), []string{deadAddr, followerAddr, leaderAddr}, makeRetryPolicy())
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	req := &pb_meta.CreateFileRequest{}
	var resp pb_meta.CreateFileResponse
	err = lc.Invoke(context.Background(), testMethod, req, &resp)
	assert.NoError(t, err)
	assert.Equal(t, "file-1", resp.GetFileId())
}

func TestInvoke_New_ConnectsToFirstAvailable(t *testing.T) {
	addr := startTestServer(t, successHandler)
	lc, err := New(context.Background(), []string{addr}, makeRetryPolicy())
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()
	assert.NotNil(t, lc.cache.Conn())
}

func TestNewStream_DelegatesToConnection(t *testing.T) {
	addr := startTestServer(t, successHandler)
	lc, err := New(context.Background(), []string{addr}, makeRetryPolicy())
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	cs, err := lc.NewStream(context.Background(), &grpc.StreamDesc{
		StreamName:    "TestStream",
		ServerStreams: true,
	}, "/test/method")
	_ = cs
	_ = err
}

func TestImplementsClientConnInterface(t *testing.T) {
	var _ grpc.ClientConnInterface = (*LeaderAwareClient)(nil)
}
