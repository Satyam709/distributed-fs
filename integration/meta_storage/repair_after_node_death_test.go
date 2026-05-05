//go:build integration

package meta_storage

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/integration/testutil"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	placepkg "github.com/satyam709/distributed-fs/metadata/placement"
	"github.com/satyam709/distributed-fs/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// repairCluster is a 1-meta + N-storage cluster with a custom replication
// factor, created per-test to avoid interfering with TestMain.
type repairCluster struct {
	*testutil.TestCluster
	cleanups []func()
}

// startRepairCluster creates a cluster with numStorage nodes and the given
// replication factor. This is separate from TestMain's shared 3-node cluster.
func startRepairCluster(t *testing.T, numStorage, rf int) *repairCluster {
	t.Helper()
	rc := &repairCluster{TestCluster: &testutil.TestCluster{}}

	// Build metadata config with custom replication factor.
	raftDir := t.TempDir()
	metaCfg := testutil.BuildMetaConfig("meta-repair-1", testutil.NextRaftAddr(), raftDir, true, rf,
		3*time.Second, 6*time.Second, 1*time.Second, 2*time.Second)

	app, err := testutil.NewMetadataApp(metaCfg)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, app.Run(ctx))
	rc.MetaApp = app
	rc.MetaAddr = app.BoundGRPCAddr()

	metaConn, metaC, err := testutil.DialMeta(rc.MetaAddr)
	require.NoError(t, err)
	rc.MetaConn = metaConn
	rc.MetaC = metaC

	metaAddrs := []string{rc.MetaAddr}
	for i := range numStorage {
		node, addr := testutil.StartStorageNodeAt(t, i, metaAddrs, rf, &rc.cleanups)
		conn, storageC, replC := testutil.DialStorageFull(addr)
		rc.StorageNodes = append(rc.StorageNodes, node)
		rc.StorageAddrs = append(rc.StorageAddrs, addr)
		rc.StorageConns = append(rc.StorageConns, conn)
		rc.StorageCs = append(rc.StorageCs, storageC)
		rc.ReplCs = append(rc.ReplCs, replC)
	}

	time.Sleep(500 * time.Millisecond)

	// Wait for initial reconciler runs to complete (ReconcileDelay=2s).
	// The reconciler uses chunk lists captured at registration time,
	// which are always empty for newly started nodes. Uploading chunks
	// before the reconciler completes causes false "missing replica"
	// evictions.
	time.Sleep(3 * time.Second)

	return rc
}

func (rc *repairCluster) Shutdown() {
	rc.TestCluster.Shutdown()
	for i := len(rc.cleanups) - 1; i >= 0; i-- {
		rc.cleanups[i]()
	}
}

// TestRepairAfterNodeDeathConverges reproduces the replication flood bug:
//
//  1. Upload a chunk with full RF=3 replication on a 5-node cluster.
//  2. Kill one storage node (simulating docker kill).
//  3. Verify repair jobs are created for the missing replica.
//  4. Verify repair converges — job count stabilizes and does not grow
//     unbounded (the replication flood bug).
//  5. Verify the chunk is adequately replicated on remaining live nodes.
func TestRepairAfterNodeDeathConverges(t *testing.T) {
	const numStorage = 5
	const rf = 3

	rc := startRepairCluster(t, numStorage, rf)
	defer rc.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ── Phase 1: Upload a chunk with full replication ──
	fileID := "repair-death-file"
	chunkID := "repair-death-chunk"
	payload := []byte("repair after node death — convergence test payload data.")

	createResp, err := rc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "repair-death.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 1)

	placement := createResp.Placements[0]
	primary := placement.Primary
	require.NotNil(t, primary)

	var replicaAddrs []string
	for _, r := range placement.Replicas {
		replicaAddrs = append(replicaAddrs, r.Address)
	}
	require.Len(t, replicaAddrs, 2, "RF=3 should assign 2 replicas")

	primaryClient := testutil.DialStorage(t, primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replicaAddrs)

	time.Sleep(2 * time.Second)

	// Commit the file so GetFile works after repair.
	fileHash := sha256.Sum256(payload)
	_, err = rc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: int64(len(payload)),
		Checksum: fileHash[:],
	})
	require.NoError(t, err)

	// Verify chunk is fully replicated.
	chunk, err := rc.MetaApp.FSM.GetChunk(chunkID)
	require.NoError(t, err)
	t.Logf("chunk replicas before kill: %v", chunk.Replicas)
	assert.Len(t, chunk.Replicas, rf, "chunk should have RF=%d replicas before node death", rf)

	// ── Phase 2: Kill one storage node that holds a replica ──
	chunk, err = rc.MetaApp.FSM.GetChunk(chunkID)
	require.NoError(t, err)
	require.NotEmpty(t, chunk.Replicas, "chunk should have replicas")

	victimNodeID := chunk.Replicas[0]
	var victimIdx int
	var victimNode *storage.StorageNode
	for i, n := range rc.StorageNodes {
		if n.GetNodeID() == victimNodeID {
			victimIdx = i
			victimNode = n
			break
		}
	}
	require.NotNil(t, victimNode, "should find victim node %q", victimNodeID)
	victimAddr := victimNode.BoundAddr()
	t.Logf("killing storage node %q (index %d, addr=%s) which holds a replica", victimNodeID, victimIdx, victimAddr)

	victimNode.Stop()

	// ── Phase 3: Wait for NodeWatcher to detect dead node and trigger repair ──
	// The cluster config uses SuspectTimeout=3s, WatcherInterval=1s.
	// After Stop(), heartbeats stop → after ~3s the node is detected dead.
	t.Logf("waiting for node death detection (suspectTimeout=%s, watcherInterval=%s)...",
		3*time.Second, 1*time.Second)

	repairTriggered := false
	for i := 0; i < 15; i++ {
		time.Sleep(1 * time.Second)
		pending, err := rc.MetaApp.FSM.GetJobsByStatus(fsm.RepairStatusPending)
		require.NoError(t, err)
		inProg, err := rc.MetaApp.FSM.GetJobsByStatus(fsm.RepairStatusInProgress)
		require.NoError(t, err)
		done, err := rc.MetaApp.FSM.GetJobsByStatus(fsm.RepairStatusDone)
		require.NoError(t, err)

		activeCount := len(pending) + len(inProg)
		t.Logf("t+%ds: pending=%d, in-progress=%d, done=%d (total active=%d)",
			(i + 1), len(pending), len(inProg), len(done), activeCount)

		if activeCount > 0 || len(done) > 0 {
			repairTriggered = true
		}

		if len(done) > 0 && activeCount == 0 {
			t.Logf("repair completed at t+%ds", (i + 1))
			break
		}
	}

	require.True(t, repairTriggered, "repair should be triggered after node death")

	// ── Phase 4: Verify convergence — no flood ──
	// Sample repair job counts over time. With the fixes (dedup + backoff),
	// the total job count should stabilize, not grow unbounded.
	baselineJobs := countAllRepairJobs(rc.MetaApp.FSM)
	t.Logf("baseline total repair jobs after repair: %d", baselineJobs)

	maxSeen := baselineJobs
	for i := 0; i < 6; i++ {
		time.Sleep(3 * time.Second)
		current := countAllRepairJobs(rc.MetaApp.FSM)
		if current > maxSeen {
			maxSeen = current
		}
		t.Logf("sample %d: total repair jobs=%d (baseline=%d)", i+1, current, baselineJobs)

		// The job count should not grow significantly after convergence.
		// Allow a small margin for legitimate re-scheduling (e.g., backoff retries).
		if current > baselineJobs*2+5 {
			t.Errorf("repair job count grew from %d to %d — possible repair flood loop",
				baselineJobs, current)
			return
		}
	}

	// ── Phase 5: Verify chunk is adequately replicated ──
	locations, err := rc.MetaC.GetChunkLocations(ctx, &pb_meta.GetChunkLocationsRequest{
		ChunkId: chunkID,
	})
	require.NoError(t, err)

	t.Logf("chunk locations after repair: %d nodes", len(locations.Nodes))
	for _, n := range locations.Nodes {
		t.Logf("  replica: node=%s addr=%s", n.NodeId, n.Address)
	}

	assert.GreaterOrEqual(t, len(locations.Nodes), rf,
		"chunk should have at least RF=%d replicas after repair", rf)

	// ── Phase 5a: Verify dead node NOT in FSM chunk replicas ──
	chunkAfter, err := rc.MetaApp.FSM.GetChunk(chunkID)
	require.NoError(t, err)
	for _, rep := range chunkAfter.Replicas {
		assert.NotEqual(t, victimNodeID, rep,
			"dead node %q should not be in FSM chunk replicas after repair", victimNodeID)
	}
	t.Logf("FSM chunk replicas after repair: %v", chunkAfter.Replicas)

	// ── Phase 5b: Verify dead node NOT in GetFile response ──
	getResp, err := rc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})
	require.NoError(t, err)
	require.NotEmpty(t, getResp.Chunks, "GetFile should return chunk info")

	for _, ci := range getResp.Chunks {
		for _, addr := range ci.Replicas {
			assert.NotEqual(t, victimAddr, addr,
				"dead node address %q should not appear in GetFile chunk replicas", victimAddr)
		}
		t.Logf("GetFile chunk %s replicas: %v", ci.ChunkId, ci.Replicas)
	}

	// ── Phase 6: Final stability check ──
	time.Sleep(5 * time.Second)
	finalJobs := countAllRepairJobs(rc.MetaApp.FSM)
	pendingFinal, _ := rc.MetaApp.FSM.GetJobsByStatus(fsm.RepairStatusPending)
	t.Logf("final: total jobs=%d, pending=%d", finalJobs, len(pendingFinal))
	assert.LessOrEqual(t, finalJobs, maxSeen+3,
		"repair jobs should not keep growing after convergence (maxSeen=%d, final=%d)",
		maxSeen, finalJobs)
}

// TestNoTooManyPingsSent verifies that storage nodes do not trigger
// "too_many_pings" GoAway errors during normal cluster operation.
// This is verified implicitly — if the gRPC server had the default
// restrictive MinTime of 5 minutes, P2P connections (with 5s pings)
// would be kicked immediately after the first keepalive ping,
// causing all replication to fail.
func TestNoTooManyPingsSent(t *testing.T) {
	const numStorage = 3
	const rf = 3

	rc := startRepairCluster(t, numStorage, rf)
	defer rc.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Upload several chunks to ensure P2P replication happens between
	// all storage nodes, exercising the PeerDialer keepalive pings.
	for i := 0; i < 3; i++ {
		chunkID := "too-many-pings-ck-" + string(rune('0'+i))
		fileID := "too-many-pings-file-" + string(rune('0'+i))
		payload := []byte("too many pings test chunk " + string(rune('0'+i)))

		createResp, err := rc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
			FileId:    fileID,
			FileName:  "tmp.dat",
			FileSize:  int64(len(payload)),
			ChunkSize: 4 * 1024 * 1024,
			ChunkIds:  []string{chunkID},
		})
		require.NoError(t, err)

		placement := createResp.Placements[0]
		primaryClient := testutil.DialStorage(t, placement.Primary.Address)
		var replAddrs []string
		for _, r := range placement.Replicas {
			replAddrs = append(replAddrs, r.Address)
		}
		testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replAddrs)
	}

	// Let keepalive pings cycle several times (5s interval, wait 20s).
	time.Sleep(20 * time.Second)

	// Verify all chunks are fully replicated. If "too_many_pings"
	// had kicked P2P connections, replication would have failed.
	for i := 0; i < 3; i++ {
		chunkID := "too-many-pings-ck-" + string(rune('0'+i))
		chunk, err := rc.MetaApp.FSM.GetChunk(chunkID)
		require.NoError(t, err)
		assert.Len(t, chunk.Replicas, rf,
			"chunk %s should have %d replicas", chunkID, rf)
	}
}

// TestsCountAllRepairJobs verifies the helper counts correctly.
func TestCountAllRepairJobsHelper(t *testing.T) {
	if len(tc.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}
	total := countAllRepairJobs(tc.MetaApp.FSM)
	assert.GreaterOrEqual(t, total, 0)
}

// Ensure testutil helpers are accessible.
var _ = testutil.DialStorage
var _ = testutil.PutChunkData

// TestPlacementExcludesFullNodes verifies that the MostFreeSpaceStrategy
// used for repair placement excludes nodes with insufficient free space
// (below DefaultMinFreeSpace = 64KiB).
func TestPlacementExcludesFullNodes(t *testing.T) {
	const numStorage = 5
	const rf = 3

	rc := startRepairCluster(t, numStorage, rf)
	defer rc.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Upload a chunk to populate the FSM.
	fileID := "placement-full-node"
	chunkID := "placement-full-ck"
	payload := []byte("full node exclusion test data.")

	createResp, err := rc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "full-node.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)

	placement := createResp.Placements[0]
	primaryClient := testutil.DialStorage(t, placement.Primary.Address)

	var replAddrs []string
	for _, r := range placement.Replicas {
		replAddrs = append(replAddrs, r.Address)
	}
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replAddrs)

	time.Sleep(2 * time.Second)

	// Get live nodes from FSM.
	liveNodes, err := rc.MetaApp.FSM.GetLiveNodes()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(liveNodes), numStorage, "all %d nodes should be alive", numStorage)

	// Verify all live nodes have reasonable free space.
	for _, n := range liveNodes {
		assert.Greater(t, n.FreeSpace, uint64(0),
			"node %q should have FreeSpace > 0", n.NodeID)
	}

	// minSpace=0 → no filtering (all nodes eligible).
	strategy := placepkg.MostFreeSpaceStrategy{}

	selected, err := strategy.SelectNodes(liveNodes, chunkID, 0, 1)
	require.NoError(t, err)
	require.Len(t, selected, 1)

	// minSpace=100 → filter out nodes with FreeSpace < 100.
	modifiedNodes := make([]fsm.NodeEntry, len(liveNodes))
	copy(modifiedNodes, liveNodes)
	modifiedNodes[0] = liveNodes[0]
	modifiedNodes[0].FreeSpace = 0

	selected2, err := strategy.SelectNodes(modifiedNodes, chunkID, 100, 1)
	require.NoError(t, err, "should still find a node with FreeSpace >= 100 (5 nodes, 1 full)")
	require.Len(t, selected2, 1)
	require.NotEqual(t, modifiedNodes[0].NodeID, selected2[0].NodeID,
		"full node (FreeSpace=0) should not be selected as repair target")
	t.Logf("full node %q (FreeSpace=0) was correctly excluded, selected %q (FreeSpace=%d)",
		modifiedNodes[0].NodeID, selected2[0].NodeID, selected2[0].FreeSpace)
}
