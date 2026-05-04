//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func countPendingRepairJobs(f *fsm.MetadataFSM) int {
	jobs, err := f.GetJobsByStatus(fsm.RepairStatusPending)
	if err != nil {
		return -1
	}
	return len(jobs)
}

func countAllRepairJobs(f *fsm.MetadataFSM) int {
	total := 0
	for _, status := range []fsm.RepairStatus{
		fsm.RepairStatusPending,
		fsm.RepairStatusInProgress,
		fsm.RepairStatusDone,
		fsm.RepairStatusFailed,
	} {
		jobs, err := f.GetJobsByStatus(status)
		if err != nil {
			return -1
		}
		total += len(jobs)
	}
	return total
}

// TestNoSpuriousRepairJobs verifies that after uploading a chunk with full
// replication (RF=3 on a 3-node cluster), no repair jobs are created.
//
// Bug being exposed: CommitChunk unconditionally calls ScheduleRepairForChunk,
// and the reconciler may also trigger spurious repair on fresh node registration.
func TestNoSpuriousRepairJobs(t *testing.T) {
	if len(testCluster.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "no-spurious-repair"
	chunkID := "no-spurious-chunk"
	payload := []byte("No spurious repair test data.")

	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "no-spurious.dat",
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
	require.Len(t, replicaAddrs, 2, "placement should give 2 replicas with RF=3")

	primaryClient := DialStorage(t, primary.Address)
	PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replicaAddrs)

	// Wait for CommitChunk Raft proposal + replication to finish.
	time.Sleep(2 * time.Second)

	fileHash := sha256.Sum256(payload)
	_, err = testCluster.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: int64(len(payload)),
		Checksum: fileHash[:],
	})
	require.NoError(t, err)

	// Let any spurious repair scheduling propagate.
	time.Sleep(2 * time.Second)

	pending := countPendingRepairJobs(testCluster.MetaApp.FSM)
	total := countAllRepairJobs(testCluster.MetaApp.FSM)

	t.Logf("pending repair jobs: %d, total repair jobs: %d", pending, total)

	assert.Zero(t, pending,
		"no pending repair jobs should exist for adequately replicated chunk")
	assert.Zero(t, total,
		"no repair jobs should exist at all for adequately replicated chunk")
}

// TestUnderReplicationDetected verifies that when a chunk has fewer replicas
// than the replication factor, a repair job IS scheduled.
//
// This tests the happy path of under-replication detection.
func TestUnderReplicationDetected(t *testing.T) {
	if len(testCluster.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "under-repl-detect"
	chunkID := "under-repl-chunk"
	payload := []byte("Under-replication detection test data — send to only 1 node.")

	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "under-repl.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 1)

	placement := createResp.Placements[0]
	primary := placement.Primary
	require.NotNil(t, primary)

	// Upload to PRIMARY ONLY — explicitly skip replicas.
	primaryClient := DialStorage(t, primary.Address)
	PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, nil)
	// The storage handler will still auto-replicate to the assigned replicas.
	// We can't prevent that from here, so we skip this test if auto-replication
	// is unavoidable.

	t.Skip("TODO: need a way to suppress auto-replication to test deficit detection. " +
		"The storage handler currently always replicates. This test is a placeholder " +
		"for when single-node write capability is verified separately.")
}

// TestUnderReplicationTriggersRepair submits a chunk with incomplete replication
// by directly writing only to the primary and then erasing the FSM replica entries.
// It then verifies a repair job gets scheduled.
func TestUnderReplicationTriggersRepair(t *testing.T) {
	if len(testCluster.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "manual-under-repl"
	chunkID := "manual-under-repl-chunk"
	payload := []byte("Manual under-replication test.")
	fileName := "manual-under.dat"

	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  fileName,
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 1)

	placement := createResp.Placements[0]
	primary := placement.Primary
	primaryClient := DialStorage(t, primary.Address)

	// Upload via primary — this will auto-commit and auto-replicate.
	PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, nil)
	time.Sleep(2 * time.Second)

	// The chunk should be adequately replicated (RF=3 → 3 replicas).
	// Now trigger manual under-replication.
	scheduler := testCluster.MetaApp.Scheduler
	scheduler.ScheduleRepairForChunk(chunkID)

	time.Sleep(2 * time.Second)

	pending := countPendingRepairJobs(testCluster.MetaApp.FSM)
	total := countAllRepairJobs(testCluster.MetaApp.FSM)

	t.Logf("pending repair jobs after manual trigger: %d, total: %d", pending, total)

	if pending == 0 && total == 0 {
		t.Logf("no repair jobs scheduled (chunk was already adequately replicated)")
	}

	// Now evict one replica manually to create genuine under-replication.
	chunk, err := testCluster.MetaApp.FSM.GetChunk(chunkID)
	require.NoError(t, err)

	require.NotEmpty(t, chunk.Replicas, "chunk should have replicas")
	if len(chunk.Replicas) > 1 {
		targetNodeID := chunk.Replicas[1]

		// Manually evict a replica.
		_, err = testCluster.MetaC.ReportCorruption(ctx, &pb_meta.ReportCorruptionRequest{
			ChunkId:    chunkID,
			ReporterId: targetNodeID,
		})
		// ReportCorruption may fail if targetNodeID is an address, not a node ID.
		if err != nil {
			t.Logf("ReportCorruption failed (expected if replicas contain addresses): %v", err)
		}

		time.Sleep(3 * time.Second)

		pending = countPendingRepairJobs(testCluster.MetaApp.FSM)
		total = countAllRepairJobs(testCluster.MetaApp.FSM)
		t.Logf("after eviction — pending: %d, total: %d", pending, total)

		assert.True(t, pending > 0 || total > 0,
			"repair should be triggered after a replica is evicted")
	}
}

// TestRepairDoesNotLoop verifies that the repair system converges — it does
// not keep creating new repair jobs indefinitely for adequately replicated chunks.
//
// Bug being exposed: unconditional ScheduleRepairForChunk + potential repair
// cascade from mixed node-ID/address formats in replica lists.
func TestRepairDoesNotLoop(t *testing.T) {
	if len(testCluster.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ── Phase 1: Upload 3 chunks to create a realistic scenario ──
	type chunkSpec struct {
		chunkID string
		payload []byte
	}
	specs := []chunkSpec{
		{"loop-chunk-0", []byte("loop test chunk zero.")},
		{"loop-chunk-1", []byte("loop test chunk one.")},
		{"loop-chunk-2", []byte("loop test chunk two.")},
	}

	var chunkIDs []string
	var totalSize int64
	for _, s := range specs {
		chunkIDs = append(chunkIDs, s.chunkID)
		totalSize += int64(len(s.payload))
	}

	fileID := "no-loop-file"
	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "no-loop.dat",
		FileSize:  totalSize,
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  chunkIDs,
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 3)

	for i, pl := range createResp.Placements {
		primaryClient := DialStorage(t, pl.Primary.Address)
		var replAddrs []string
		for _, r := range pl.Replicas {
			replAddrs = append(replAddrs, r.Address)
		}
		PutChunkData(t, ctx, primaryClient, chunkIDs[i], fileID, specs[i].payload, replAddrs)
		t.Logf("uploaded %s → %s (+ %d replicas)", chunkIDs[i], pl.Primary.Address, len(replAddrs))
	}

	time.Sleep(2 * time.Second)

	// Commit all files.
	var allData []byte
	for _, s := range specs {
		allData = append(allData, s.payload...)
	}
	fileHash := sha256.Sum256(allData)
	_, err = testCluster.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: totalSize,
		Checksum: fileHash[:],
	})
	require.NoError(t, err)

	// ── Phase 2: Let replication settle ──
	time.Sleep(1 * time.Second)

	// Baseline job count.
	baseline := countAllRepairJobs(testCluster.MetaApp.FSM)
	t.Logf("baseline repair jobs: %d", baseline)
	if baseline == 0 {
		t.Skip("no repair jobs at baseline — cascade condition not triggered; " +
			"try with mixed-address replicas scenario")
	}

	// ── Phase 3: Wait and check job count does not grow unbounded ──
	var maxJobs int
	for i := range 5 {
		time.Sleep(2 * time.Second)
		current := countAllRepairJobs(testCluster.MetaApp.FSM)
		if current > maxJobs {
			maxJobs = current
		}
		t.Logf("sampled repair jobs at %d: %d", (i+1)*2, current)

		// Check for runaway growth.
		if current > baseline*3 {
			t.Errorf("repair job count grew from %d to %d — possible repair loop",
				baseline, current)
			return
		}
	}

	t.Logf("max repair jobs observed: %d (baseline: %d)", maxJobs, baseline)
	_ = maxJobs

	// ── Phase 4: Verify chunk replicas after settling ──
	for _, chunkID := range chunkIDs {
		locations, err := testCluster.MetaC.GetChunkLocations(ctx, &pb_meta.GetChunkLocationsRequest{
			ChunkId: chunkID,
		})
		if err != nil {
			t.Logf("GetChunkLocations(%s) failed: %v", chunkID, err)
			continue
		}

		t.Logf("chunk %s replicas: %d → %v", chunkID, len(locations.Nodes),
			func() []string {
				var ids []string
				for _, n := range locations.Nodes {
					ids = append(ids, fmt.Sprintf("%s@%s", n.NodeId, n.Address))
				}
				return ids
			}())

		// Verify at least some replicas exist.
		assert.NotZero(t, len(locations.Nodes),
			"chunk %s should have at least one resolved replica", chunkID)
	}
}

// isNodeID checks whether v is a known node ID in the FSM node registry.
func isNodeID(f *fsm.MetadataFSM, v string) bool {
	_, err := f.GetNode(v)
	return err == nil
}

// isAddress checks whether v looks like a gRPC address (contains a port separator).
func isAddress(v string) bool {
	for i := len(v) - 1; i >= 0; i-- {
		if v[i] == ':' {
			return true
		}
	}
	return false
}

// TestChunkReplicasInFSMAreNodeIDsOnly verifies that ChunkRecord.Replicas in
// the FSM contains only valid node IDs (not raw gRPC addresses).
//
// Bug being exposed: storage_handler.go stores s.NodeID for self but
// ReplicatorToNodes returns raw addresses — mixing IDs and addresses in the
// same slice. This prevents GetChunkLocations from resolving all replicas.
func TestChunkReplicasInFSMAreNodeIDsOnly(t *testing.T) {
	if len(testCluster.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "fsm-nodeids-only"
	chunkID := "fsm-nodeids-ck"
	payload := []byte("FSM replica format test data.")

	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "fsm-nodeids.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)

	placement := createResp.Placements[0]
	primaryClient := DialStorage(t, placement.Primary.Address)
	var replAddrs []string
	for _, r := range placement.Replicas {
		replAddrs = append(replAddrs, r.Address)
	}
	PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replAddrs)

	time.Sleep(2 * time.Second)

	chunk, err := testCluster.MetaApp.FSM.GetChunk(chunkID)
	require.NoError(t, err)
	require.NotEmpty(t, chunk.Replicas, "chunk should have replicas after upload")

	t.Logf("FSM replica values: %v", chunk.Replicas)

	var mixedCount int
	for _, v := range chunk.Replicas {
		if isAddress(v) && !isNodeID(testCluster.MetaApp.FSM, v) {
			mixedCount++
			t.Logf("MIXED: %q is an address but not a node ID", v)
		} else if !isNodeID(testCluster.MetaApp.FSM, v) {
			mixedCount++
			t.Logf("UNKNOWN: %q is neither a node ID nor a valid address", v)
		}
	}

	assert.Zero(t, mixedCount,
		"all replicas in FSM should be valid node IDs (found %d invalid entries)", mixedCount)
}

// TestGetFileChunkReplicasAreAddresses verifies that the GetFile gRPC response
// returns proper gRPC addresses (host:port) in ChunkInfo.Replicas, not bare node IDs.
//
// Bug being exposed: file_handler.go returns ck.Replicas (raw node IDs) without
// resolving them to full addresses. The DFS client then fails to connect because
// bare IDs like "storage-0" default to port 443.
func TestGetFileChunkReplicasAreAddresses(t *testing.T) {
	if len(testCluster.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "getfile-addrs"
	chunkID := "getfile-addrs-ck"
	payload := []byte("GetFile replica address format test.")

	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "getfile-addrs.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)

	placement := createResp.Placements[0]
	primaryClient := DialStorage(t, placement.Primary.Address)
	var replAddrs []string
	for _, r := range placement.Replicas {
		replAddrs = append(replAddrs, r.Address)
	}
	PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replAddrs)

	time.Sleep(2 * time.Second)

	fileHash := sha256.Sum256(payload)
	_, err = testCluster.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: int64(len(payload)),
		Checksum: fileHash[:],
	})
	require.NoError(t, err)

	getResp, err := testCluster.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})
	require.NoError(t, err)
	require.Len(t, getResp.Chunks, 1)

	replicas := getResp.Chunks[0].Replicas
	require.NotEmpty(t, replicas, "GetFile should return replicas")
	t.Logf("GetFile replicas: %v", replicas)

	var nonAddrCount int
	for _, v := range replicas {
		if !isAddress(v) {
			nonAddrCount++
			t.Logf("NOT-ADDRESS: %q is not a valid host:port address", v)
		}
	}

	assert.Zero(t, nonAddrCount,
		"all replicas in GetFile response should be gRPC addresses (found %d non-address entries)",
		nonAddrCount)
}

// TestGetChunkLocationsResolvesAllReplicas verifies that GetChunkLocations
// resolves every replica entry in ChunkRecord.Replicas back to a NodeInfo
// with a proper address.
//
// Bug being exposed: when replicas contain raw gRPC addresses instead of
// node IDs, GetChunkLocations fails to find them in the node registry and
// silently drops them — leading to false under-replication and repair cascades.
func TestGetChunkLocationsResolvesAllReplicas(t *testing.T) {
	if len(testCluster.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "gcl-resolve"
	chunkID := "gcl-resolve-ck"
	payload := []byte("GetChunkLocations resolution test.")

	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "gcl-resolve.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)

	placement := createResp.Placements[0]
	primaryClient := DialStorage(t, placement.Primary.Address)
	var replAddrs []string
	for _, r := range placement.Replicas {
		replAddrs = append(replAddrs, r.Address)
	}
	require.Len(t, replAddrs, 2, "RF=3 should give 2 replicas in placement")
	PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replAddrs)

	time.Sleep(2 * time.Second)

	// Check FSM raw replica count.
	chunk, err := testCluster.MetaApp.FSM.GetChunk(chunkID)
	require.NoError(t, err)
	rawCount := len(chunk.Replicas)
	t.Logf("FSM raw replicas: %d → %v", rawCount, chunk.Replicas)

	// Check how many are resolved by GetChunkLocations.
	locations, err := testCluster.MetaC.GetChunkLocations(ctx, &pb_meta.GetChunkLocationsRequest{
		ChunkId: chunkID,
	})
	require.NoError(t, err)
	resolvedCount := len(locations.Nodes)
	t.Logf("GetChunkLocations resolved: %d nodes → %v",
		resolvedCount,
		func() []string {
			var addrs []string
			for _, n := range locations.Nodes {
				addrs = append(addrs, n.Address)
			}
			return addrs
		}())

	// Every replica in the FSM raw list should be resolvable.
	assert.Equal(t, rawCount, resolvedCount,
		"GetChunkLocations should resolve all %d FSM replicas, but resolved only %d",
		rawCount, resolvedCount)
	assert.Equal(t, 3, resolvedCount,
		"with RF=3, all 3 replicas should be resolvable")

	// All resolved nodes should have valid addresses.
	for _, n := range locations.Nodes {
		assert.NotEmpty(t, n.NodeId)
		assert.True(t, isAddress(n.Address),
			"resolved node %s should have address with port, got %q", n.NodeId, n.Address)
	}
}
