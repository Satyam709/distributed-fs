package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type testProposer struct {
	fsm *fsm.MetadataFSM
}

func (tp *testProposer) Propose(cmd fsm.MetadataCommand) error {
	raw, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	if result := tp.fsm.Apply(&raft.Log{Data: raw}); result != nil {
		if e, ok := result.(error); ok {
			return e
		}
	}
	return nil
}

type testRepairer struct {
	calls []string
}

func (tr *testRepairer) ScheduleRepairForChunk(chunkID string) {
	tr.calls = append(tr.calls, chunkID)
}

type testStorageClient struct {
	calls    []deleteCall
	failNext bool
	failErr  error
}

type deleteCall struct {
	addr    string
	chunkID string
}

func (tsc *testStorageClient) DeleteChunk(ctx context.Context, addr, chunkID string) error {
	tsc.calls = append(tsc.calls, deleteCall{addr: addr, chunkID: chunkID})
	if tsc.failNext {
		tsc.failNext = false
		if tsc.failErr != nil {
			return tsc.failErr
		}
		return errors.New("mock delete failure")
	}
	return nil
}

func applyFSMCommand(t *testing.T, m *fsm.MetadataFSM, typ fsm.MetadataCmdType, payload any) {
	t.Helper()

	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	cmdRaw, err := json.Marshal(fsm.MetadataCommand{Type: typ, Payload: raw})
	require.NoError(t, err)

	if out := m.Apply(&raft.Log{Data: cmdRaw}); out != nil {
		require.NoError(t, out.(error))
	}
}

func setupFSMWithNodesAndChunks(t *testing.T) *fsm.MetadataFSM {
	t.Helper()
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	now := time.Now()

	// Register 3 nodes.
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-a", Address: "10.0.0.1:4000", FreeSpace: 1024, ChunkCount: 2, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-b", Address: "10.0.0.2:4000", FreeSpace: 1024, ChunkCount: 2, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-c", Address: "10.0.0.3:4000", FreeSpace: 1024, ChunkCount: 2, CreatedAt: now,
	})

	// Create a file with 2 chunks.
	applyFSMCommand(t, m, fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID:    "file-1",
		FileName:  "f.bin",
		ChunkIDs:  []string{"chunk-1", "chunk-2"},
		FileSize:  2,
		CreatedAt: now,
	})

	// Commit chunks with replicas on all 3 nodes (RF=3).
	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID:  "chunk-1",
		NodeIDs:  []string{"node-a", "node-b", "node-c"},
		Checksum: []byte("chk1"),
	})
	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID:  "chunk-2",
		NodeIDs:  []string{"node-a", "node-b", "node-c"},
		Checksum: []byte("chk2"),
	})

	return m
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestReconcileNode_NoDiscrepancy(t *testing.T) {
	m := setupFSMWithNodesAndChunks(t)
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)
	r.ReconcileNode("node-a", []string{"chunk-1", "chunk-2"})

	assert.Empty(t, storage.calls)
	assert.Empty(t, repairer.calls)

	// Verify no replicas were removed.
	loc, err := m.GetChunkLocations("chunk-1")
	require.NoError(t, err)
	assert.Len(t, loc, 3)
}

func TestReconcileNode_StaleReplica(t *testing.T) {
	m := setupFSMWithNodesAndChunks(t)
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)
	// node-a reports chunk-99 which FSM does not know about.
	r.ReconcileNode("node-a", []string{"chunk-1", "chunk-2", "chunk-99"})

	require.Len(t, storage.calls, 1)
	assert.Equal(t, "10.0.0.1:4000", storage.calls[0].addr)
	assert.Equal(t, "chunk-99", storage.calls[0].chunkID)

	// Repair should NOT be triggered for a stale replica.
	assert.Empty(t, repairer.calls)
}

func TestReconcileNode_MissingReplica_UnderReplicated(t *testing.T) {
	m := setupFSMWithNodesAndChunks(t)
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)
	// node-a reports only chunk-1; chunk-2 is missing.
	// After evicting node-a from chunk-2, only node-b and node-c remain
	// => 2 live replicas < RF(3) => repair should be scheduled.
	r.ReconcileNode("node-a", []string{"chunk-1"})

	// No DeleteChunk calls for missing replicas.
	assert.Empty(t, storage.calls)

	// Repair should be triggered for chunk-2.
	require.Len(t, repairer.calls, 1)
	assert.Equal(t, "chunk-2", repairer.calls[0])

	// Verify node-a was evicted from chunk-2.
	loc, err := m.GetChunkLocations("chunk-2")
	require.NoError(t, err)
	assert.Len(t, loc, 2)
}

func TestReconcileNode_MissingReplica_StillReplicated(t *testing.T) {
	m := setupFSMWithNodesAndChunks(t)
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	// Change RF to 2 so that after eviction we are still adequately replicated.
	r, err := NewReconciler(m, proposer, repairer, storage, 0, 2)
	require.NoError(t, err)
	r.ReconcileNode("node-a", []string{"chunk-1"})

	// No repair because 2 remaining replicas == RF.
	assert.Empty(t, repairer.calls)

	// But node-a should still be evicted from chunk-2.
	loc, err := m.GetChunkLocations("chunk-2")
	require.NoError(t, err)
	assert.Len(t, loc, 2)
}

func TestReconcileNode_Mixed(t *testing.T) {
	m := setupFSMWithNodesAndChunks(t)
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)

	// node-a has stale chunk-99 and is missing chunk-2.
	r.ReconcileNode("node-a", []string{"chunk-1", "chunk-99"})

	require.Len(t, storage.calls, 1)
	assert.Equal(t, "chunk-99", storage.calls[0].chunkID)

	require.Len(t, repairer.calls, 1)
	assert.Equal(t, "chunk-2", repairer.calls[0])
}

func TestReconcileNode_NodeNotFound(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)

	r.ReconcileNode("ghost-node", []string{"chunk-1"})

	assert.Empty(t, storage.calls)
	assert.Empty(t, repairer.calls)
}

func TestReconcileNode_DeleteChunkFailure(t *testing.T) {
	m := setupFSMWithNodesAndChunks(t)
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{failNext: true, failErr: errors.New("conn refused")}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)

	r.ReconcileNode("node-a", []string{"chunk-1", "chunk-2", "chunk-99"})

	// DeleteChunk was attempted and failed.
	require.Len(t, storage.calls, 1)
	assert.Equal(t, "chunk-99", storage.calls[0].chunkID)

	// Despite RPC failure, the FSM eviction was still proposed.
	// chunk-99 is not in the FSM anyway, but the proposer ran.
	assert.Empty(t, repairer.calls)
}

func TestReconcileNode_MultipleStaleChunks(t *testing.T) {
	m := setupFSMWithNodesAndChunks(t)
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)

	r.ReconcileNode("node-a", []string{"chunk-1", "chunk-2", "chunk-x", "chunk-y"})

	require.Len(t, storage.calls, 2)
	assert.Equal(t, "chunk-x", storage.calls[0].chunkID)
	assert.Equal(t, "chunk-y", storage.calls[1].chunkID)
}

func TestSchedule_CancelsOldTimer(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)

	r.reconcileDelay = 50 * time.Millisecond

	r.Schedule("node-a", []string{"chunk-1"})
	r.Schedule("node-a", []string{"chunk-2"}) // should cancel first timer

	// Immediately after the second Schedule, the new timer should be in the map.
	r.mu.Lock()
	_, exists := r.timers["node-a"]
	r.mu.Unlock()
	assert.True(t, exists, "second timer should still be in map")

	// Wait for the timer to fire.
	time.Sleep(150 * time.Millisecond)

	r.mu.Lock()
	_, exists = r.timers["node-a"]
	r.mu.Unlock()
	assert.False(t, exists, "timer should have fired and been deleted")
}

func TestCancel_StopsTimer(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	proposer := &testProposer{fsm: m}
	repairer := &testRepairer{}
	storage := &testStorageClient{}

	r, err := NewReconciler(m, proposer, repairer, storage, 0, 3)
	require.NoError(t, err)

	r.Schedule("node-a", []string{"chunk-1"})
	r.Cancel("node-a")

	r.mu.Lock()
	_, exists := r.timers["node-a"]
	r.mu.Unlock()
	assert.False(t, exists)
}
