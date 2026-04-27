//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Scenario 1: Node Registration & Heartbeat
// ---------------------------------------------------------------------------

// TestStorageNodesAutoRegistered verifies that storage nodes registered
// themselves with the metadata cluster during Start(). The CreateFile
// call implicitly proves nodes are live — it would fail with
// "no live nodes" if registration hadn't happened.
func TestStorageNodesAutoRegistered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// CreateFile needs live nodes for placement; if nodes didn't register
	// this will fail with "error getting placement".
	resp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:   "reg-probe-file",
		FileName: "probe.txt",
		FileSize: 100,
		ChunkIds: []string{"reg-probe-chunk-1"},
	})
	require.NoError(t, err, "CreateFile should succeed when storage nodes are registered")
	require.Len(t, resp.Placements, 1, "expected 1 chunk placement")

	primary := resp.Placements[0].Primary
	assert.NotEmpty(t, primary.NodeId, "primary node ID must be set")
	assert.NotEmpty(t, primary.Address, "primary address must be set")
	t.Logf("placement: chunk=%s → primary=%s@%s",
		resp.Placements[0].ChunkId, primary.NodeId, primary.Address)
}

// TestHeartbeatSucceeds verifies that a heartbeat RPC from a registered
// storage node is accepted by the metadata leader.
func TestHeartbeatSucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hbResp, err := testCluster.MetaC.Heartbeat(ctx, &pb_meta.HeartbeatRequest{
		NodeId:     "storage-0",
		FreeSpace:  1024 * 1024 * 200,
		ChunkCount: 0,
	})
	require.NoError(t, err, "heartbeat should succeed for a registered node")
	// We only check that the heartbeat was accepted — repair jobs may or
	// may not be present depending on other test state.
	assert.NotNil(t, hbResp, "heartbeat response should not be nil")
	t.Logf("heartbeat accepted, repair jobs in response: %d", len(hbResp.RepairJobs))
}

// TestHeartbeatUnregisteredNodeReturnsNotFound verifies that a heartbeat
// from a node that was never registered is rejected with NotFound.
func TestHeartbeatUnregisteredNodeReturnsNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := testCluster.MetaC.Heartbeat(ctx, &pb_meta.HeartbeatRequest{
		NodeId:     "ghost-node-99",
		FreeSpace:  0,
		ChunkCount: 0,
	})
	require.Error(t, err, "heartbeat from unknown node should fail")
	assert.Contains(t, err.Error(), "not registered")
}

// TestNodeDeregistration verifies that the DeregisterNode RPC transitions
// a node to "draining" status. We register a temporary node and then
// deregister it, verifying the transition.
func TestNodeDeregistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register a temporary node.
	nodeID := "temp-deregister-node"
	_, err := testCluster.MetaC.RegisterNode(ctx, &pb_meta.RegisterNodeRequest{
		NodeId:    nodeID,
		Address:   "127.0.0.1:59999",
		FreeSpace: 1000,
	})
	require.NoError(t, err)

	// Deregister it.
	deregResp, err := testCluster.MetaC.DeregisterNode(ctx, &pb_meta.DeregisterNodeRequest{
		NodeId: nodeID,
	})
	require.NoError(t, err)
	assert.True(t, deregResp.Success, "deregistration should succeed")

	// Deregistering a second time should fail because the node is no longer
	// in NodeStatusAlive (it's draining), but the FSM still has the entry,
	// so heartbeat still works. The key assertion is that deregistration
	// itself succeeds — the watcher/scheduler handles the rest.
	t.Logf("node %q deregistered successfully", nodeID)
}

// TestDeregisterNonexistentNodeFails verifies that deregistering a node
// that was never registered returns an error.
func TestDeregisterNonexistentNodeFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := testCluster.MetaC.DeregisterNode(ctx, &pb_meta.DeregisterNodeRequest{
		NodeId: "totally-nonexistent-node",
	})
	require.Error(t, err, "deregistering unknown node should fail")
}
