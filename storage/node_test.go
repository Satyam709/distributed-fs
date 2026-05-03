package storage

import (
	"testing"
	"time"

	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeNode(t *testing.T, addr string) *StorageNode {
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

	node, err := NewStorageNode(
		StorageNodeConfig{
			GRPCAddr:          addr,
			Timeout:           5 * time.Second,
			NodeID:            "test-node-1",
			MetadataAddrs:     []string{":3000"},
			DataDir:           dir,
			ReplicationFactor: 3,
			HeartbeatInterval: 3 * time.Second,
			RetryMaxAttempts:  1,
			RetryBaseBackoff:  10 * time.Millisecond,
			RetryMaxBackoff:   100 * time.Millisecond,
			RPCTimeout:        5 * time.Second,
		},
		logging.NewCLogger(),
		ds,
		&metaclient.MockMetaForNode{},
	)
	require.NoError(t, err)
	return node
}

// ---------------------------------------------------------------------------
// StorageNode tests
// ---------------------------------------------------------------------------

// TestStorageNode_StartAndStop verifies that the node binds a listener,
// serves gRPC traffic (just that Start doesn't error), and shuts down cleanly.
func TestStorageNode_StartAndStop(t *testing.T) {
	node := makeNode(t, ":0") // OS picks an available port
	require.NoError(t, node.Start())
	node.Stop() // must not hang or panic
}

// TestStorageNode_Stop_BeforeStart checks that calling Stop on a node that
// was never started is a safe no-op.
func TestStorageNode_Stop_BeforeStart(t *testing.T) {
	node := makeNode(t, ":0")
	assert.NotPanics(t, node.Stop, "Stop before Start must be a no-op")
}

// TestStorageNode_Start_InvalidPort ensures Start returns a non-nil error
// when the bind address is invalid/unusable.
func TestStorageNode_Start_InvalidPort(t *testing.T) {
	node := makeNode(t, "invalid-address-!!!")
	err := node.Start()
	assert.Error(t, err, "Start with bad address should return an error")
}

// TestStorageNode_Stop_Idempotent confirms multiple Stop calls after Start
// don't panic or deadlock.
func TestStorageNode_Stop_Idempotent(t *testing.T) {
	node := makeNode(t, ":0")
	require.NoError(t, node.Start())
	assert.NotPanics(t, node.Stop)
	// Second Stop: grpcServer is stopped already; the nil-guard in Stop
	// prevents a second GracefulStop call — should be safe.
	assert.NotPanics(t, node.Stop)
}

// TestStorageNode_AdvertiseAddr_GetGrpcAddr verifies that when AdvertiseAddr
// is configured, GetGrpcAddr returns it instead of the actual listener address.
func TestStorageNode_AdvertiseAddr_GetGrpcAddr(t *testing.T) {
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

	node, err := NewStorageNode(
		StorageNodeConfig{
			GRPCAddr:          ":0",
			AdvertiseAddr:     "storage-1:4000",
			Timeout:           5 * time.Second,
			NodeID:            "test-node-advertise",
			MetadataAddrs:     []string{":3000"},
			DataDir:           dir,
			ReplicationFactor: 3,
			HeartbeatInterval: 3 * time.Second,
			RetryMaxAttempts:  1,
			RetryBaseBackoff:  10 * time.Millisecond,
			RetryMaxBackoff:   100 * time.Millisecond,
			RPCTimeout:        5 * time.Second,
		},
		logging.NewCLogger(),
		ds,
		&metaclient.MockMetaForNode{},
	)
	require.NoError(t, err)

	require.NoError(t, node.Start())
	defer node.Stop()

	assert.Equal(t, "storage-1:4000", node.GetGrpcAddr(),
		"GetGrpcAddr should return AdvertiseAddr when set")
	assert.NotEmpty(t, node.BoundAddr(),
		"BoundAddr should return the real listener address")
	assert.NotEqual(t, node.GetGrpcAddr(), node.BoundAddr(),
		"AdvertiseAddr should differ from actual listener addr")
}
