//go:build integration

package failover

import (
	"context"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	testutil "github.com/satyam709/distributed-fs/integration/testutil"
	"github.com/satyam709/distributed-fs/internal/leaderclient"
	"github.com/satyam709/distributed-fs/internal/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestFollowerRedirect_ReturnsFailedPrecondition verifies that a raw gRPC
// client calling a follower node receives a FailedPrecondition error with
// the x-leader-grpc-addr trailer pointing to the current leader's gRPC address.
func TestFollowerRedirect_ReturnsFailedPrecondition(t *testing.T) {
	t.Parallel()

	mc := testutil.StartMultiMetadataCluster(t, 3)
	defer mc.Shutdown()

	time.Sleep(1 * time.Second)

	followers := mc.FollowerIndices()
	require.NotEmpty(t, followers, "need at least one follower")

	followerConn := mc.Conns[followers[0]]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var trailer metadata.MD
	var resp pb_meta.ListFilesResponse
	err := followerConn.Invoke(ctx, "/proto.metadata.v1.MetadataService/ListFiles",
		&pb_meta.ListFilesRequest{},
		&resp,
		grpc.Trailer(&trailer),
	)

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())

	leaderAddrs := trailer.Get("x-leader-grpc-addr")
	if assert.NotEmpty(t, leaderAddrs, "trailer must contain x-leader-grpc-addr") {
		leaderIdx := mc.LeaderIndex()
		assert.Equal(t, mc.Addrs[leaderIdx], leaderAddrs[0],
			"trailer should point to current leader gRPC addr")
	}
}

// TestLeaderAwareClient_FollowsRedirect verifies that LeaderAwareClient
// transparently follows a leader redirect from a follower node.
func TestLeaderAwareClient_FollowsRedirect(t *testing.T) {
	t.Parallel()

	mc := testutil.StartMultiMetadataCluster(t, 3)
	defer mc.Shutdown()

	leaderIdx := mc.LeaderIndex()
	require.GreaterOrEqual(t, leaderIdx, 0, "cluster must have a leader")
	time.Sleep(1 * time.Second)

	followers := mc.FollowerIndices()
	require.NotEmpty(t, followers)

	followerAddr := mc.Addrs[followers[0]]
	t.Logf("leader=%s follower=%s", mc.Addrs[leaderIdx], followerAddr)

	rp := retry.Policy{
		MaxAttempts: 5,
		Base:        100 * time.Millisecond,
		Max:         1 * time.Second,
		Multiplier:  2.0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	lc, err := leaderclient.New(ctx, []string{followerAddr}, rp)
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	metaClient := pb_meta.NewMetadataServiceClient(lc)

	resp, err := metaClient.ListFiles(ctx, &pb_meta.ListFilesRequest{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

// TestLeaderCrash_ClientRecovers verifies that when the leader crashes,
// the LeaderAwareClient follows the redirect to the new leader and
// operations continue successfully.
func TestLeaderCrash_ClientRecovers(t *testing.T) {
	t.Parallel()

	mc := testutil.StartMultiMetadataCluster(t, 3)
	defer mc.Shutdown()

	leaderIdx := mc.LeaderIndex()
	require.GreaterOrEqual(t, leaderIdx, 0, "cluster must have a leader")
	leaderApp := mc.Apps[leaderIdx]
	time.Sleep(1 * time.Second)

	rp := retry.Policy{
		MaxAttempts: 5,
		Base:        100 * time.Millisecond,
		Max:         2 * time.Second,
		Multiplier:  2.0,
	}

	ctx := context.Background()
	lc, err := leaderclient.New(ctx, mc.Addrs, rp)
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	metaClient := pb_meta.NewMetadataServiceClient(lc)

	// Perform a successful operation before the crash.
	resp, err := metaClient.ListFiles(ctx, &pb_meta.ListFilesRequest{})
	require.NoError(t, err)
	t.Logf("pre-crash ListFiles: %d files", len(resp.GetFiles()))

	// Crash the leader.
	t.Logf("shutting down leader at index %d", leaderIdx)
	_ = leaderApp.Shutdown(context.Background())
	time.Sleep(5 * time.Second)

	// Try another operation — LeaderAwareClient should redirect to new leader.
	resp2, err := metaClient.ListFiles(ctx, &pb_meta.ListFilesRequest{})
	require.NoError(t, err)
	t.Logf("post-crash ListFiles: %d files", len(resp2.GetFiles()))
}

// TestLeaderCrash_ReadAfterFailover verifies that read operations survive
// a leader crash via LeaderAwareClient redirect.
func TestLeaderCrash_ReadAfterFailover(t *testing.T) {
	t.Parallel()

	mc := testutil.StartMultiMetadataCluster(t, 3)
	defer mc.Shutdown()

	leaderIdx := mc.LeaderIndex()
	require.GreaterOrEqual(t, leaderIdx, 0)
	leaderApp := mc.Apps[leaderIdx]
	time.Sleep(1 * time.Second)

	rp := retry.Policy{
		MaxAttempts: 5,
		Base:        100 * time.Millisecond,
		Max:         2 * time.Second,
		Multiplier:  2.0,
	}

	lc, err := leaderclient.New(context.Background(), mc.Addrs, rp)
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	metaClient := pb_meta.NewMetadataServiceClient(lc)

	// Verify operation works on original leader.
	resp, err := metaClient.ListFiles(context.Background(), &pb_meta.ListFilesRequest{})
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Crash the leader.
	_ = leaderApp.Shutdown(context.Background())
	time.Sleep(5 * time.Second)

	// Operation should succeed on new leader via redirect.
	resp2, err := metaClient.ListFiles(context.Background(), &pb_meta.ListFilesRequest{})
	require.NoError(t, err)
	assert.NotNil(t, resp2)
}
