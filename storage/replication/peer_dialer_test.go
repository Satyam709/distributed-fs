package replication_test

import (
	"testing"

	"github.com/satyam709/distributed-fs/storage/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestPeerDialer_Get_CachesConnection verifies that two calls to the same
// address return the same *grpc.ClientConn object (no redundant dials).
func TestPeerDialer_Get_CachesConnection(t *testing.T) {
	d := replication.NewPeerDialer()
	defer d.CloseAll()

	// Use a bufconn address — the connection will be in IDLE state which is healthy.
	// We use a well-known but unreachable address; NewClient is non-blocking so
	// no error is returned and the conn enters IDLE (healthy).
	addr := "127.0.0.1:0"
	conn1, err := d.Get(addr)
	require.NoError(t, err)
	require.NotNil(t, conn1)

	conn2, err := d.Get(addr)
	require.NoError(t, err)
	// Same pointer — cached connection returned.
	assert.Same(t, conn1, conn2)
}

// TestPeerDialer_Remove_Evicts verifies that Remove forces a re-dial on the
// next Get call (different connection object).
func TestPeerDialer_Remove_Evicts(t *testing.T) {
	d := replication.NewPeerDialer()
	defer d.CloseAll()

	addr := "127.0.0.1:0"
	conn1, err := d.Get(addr)
	require.NoError(t, err)

	d.Remove(addr)

	conn2, err := d.Get(addr)
	require.NoError(t, err)
	// Different pointer — re-dialed after eviction.
	assert.NotSame(t, conn1, conn2)
}

// TestPeerDialer_CloseAll_Safe verifies that CloseAll is safe to call even
// with open connections and does not panic.
func TestPeerDialer_CloseAll_Safe(t *testing.T) {
	d := replication.NewPeerDialer()

	_, err := d.Get("127.0.0.1:0")
	require.NoError(t, err)

	assert.NotPanics(t, d.CloseAll)
	// Double call should also be safe.
	assert.NotPanics(t, d.CloseAll)
}

// TestPeerDialer_CustomDialOpts verifies that extra grpc.DialOption are
// accepted (e.g. insecure credentials forwarded by test helpers).
func TestPeerDialer_CustomDialOpts(t *testing.T) {
	d := replication.NewPeerDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer d.CloseAll()

	conn, err := d.Get("127.0.0.1:0")
	require.NoError(t, err)
	assert.NotNil(t, conn)
}
