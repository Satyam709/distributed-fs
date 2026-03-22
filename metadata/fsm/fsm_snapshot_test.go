package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSnapshotSink is an in-memory raft.SnapshotSink used for testing
// Persist without touching the file system.
type mockSnapshotSink struct {
	buf       bytes.Buffer
	closed    bool
	cancelled bool
}

func (m *mockSnapshotSink) Write(p []byte) (int, error) {
	return m.buf.Write(p)
}
func (m *mockSnapshotSink) Close() error {
	m.closed = true
	return nil
}
func (m *mockSnapshotSink) Cancel() error {
	m.cancelled = true
	return nil
}
func (m *mockSnapshotSink) ID() string { return "mock-sink-id" }

// failingSink is a mock sink that returns an error on Write so we can
// exercise the cancel-on-error path inside Persist.
type failingSink struct {
	cancelled bool
}

func (f *failingSink) Write([]byte) (int, error) { return 0, errors.New("disk full") }
func (f *failingSink) Close() error              { return nil }
func (f *failingSink) Cancel() error             { f.cancelled = true; return nil }
func (f *failingSink) ID() string                { return "failing-sink" }

// shortWriteSink writes only the first byte of every write, simulating
// a short write.
type shortWriteSink struct {
	buf       bytes.Buffer
	cancelled bool
}

func (s *shortWriteSink) Write(p []byte) (int, error) {
	if len(p) > 1 {
		s.buf.Write(p[:1])
		return 1, nil // report 1 written, but io.Copy will see mismatch
	}
	return s.buf.Write(p)
}
func (s *shortWriteSink) Close() error  { return nil }
func (s *shortWriteSink) Cancel() error { s.cancelled = true; return nil }
func (s *shortWriteSink) ID() string    { return "short-write-sink" }

// seedFullFSM populates the FSM with representative data across all four
// registries, giving us a realistic snapshot to round-trip.
func seedFullFSM(t *testing.T, m *MetadataFSM) {
	t.Helper()
	now := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	// Nodes
	m.NodeRegistry["node-1"] = &NodeEntry{
		NodeID: "node-1", Address: "10.0.0.1:9000",
		Status: NodeStatusAlive, FreeSpace: 5000, ChunkCount: 10,
		RegisteredAt: now, UpdatedAt: now,
	}
	m.NodeRegistry["node-2"] = &NodeEntry{
		NodeID: "node-2", Address: "10.0.0.2:9000",
		Status: NodeStatusDead, FreeSpace: 0, ChunkCount: 5,
		RegisteredAt: now, UpdatedAt: now,
	}

	// Files
	m.FileIndex["file-alpha"] = &FileRecord{
		FileID: "file-alpha", Filename: "alpha.txt",
		FileSize: 1024, ChunkIDs: []string{"ck-1", "ck-2"},
		Status: FileStatusComplete, CheckSum: []byte("abc123"),
		CreatedAt: now,
	}
	m.FileIndex["file-beta"] = &FileRecord{
		FileID: "file-beta", Filename: "beta.bin",
		FileSize: 2048, ChunkIDs: []string{"ck-3"},
		Status:    FileStatusCreating,
		CreatedAt: now,
	}

	// Chunks
	m.ChunkRegistry["ck-1"] = &ChunkRecord{
		ChunkID: "ck-1", FileID: "file-alpha", ChunkIndex: 0,
		Size: 512, Checksum: []byte("sum1"),
		Replicas: []string{"node-1", "node-2"}, Status: ChunkStatusComplete,
	}
	m.ChunkRegistry["ck-2"] = &ChunkRecord{
		ChunkID: "ck-2", FileID: "file-alpha", ChunkIndex: 1,
		Size: 512, Checksum: []byte("sum2"),
		Replicas: []string{"node-1"}, Status: ChunkStatusComplete,
	}
	m.ChunkRegistry["ck-3"] = &ChunkRecord{
		ChunkID: "ck-3", FileID: "file-beta", ChunkIndex: 0,
		Status: ChunkStatusRequestAllocation,
	}

	// Repair jobs
	m.RepairJobRegistry["job-1"] = &RepairJob{
		JobID: "job-1", ChunkID: "ck-1",
		SourceNodeID: "node-1", TargetNodeID: "node-2",
		Status: RepairStatusPending, Attempts: 0,
		CreatedAt: now, UpdatedAt: now,
	}
	m.RepairJobRegistry["job-2"] = &RepairJob{
		JobID: "job-2", ChunkID: "ck-2",
		Status: RepairStatusDone, Attempts: 1, Error: "",
		CreatedAt: now, UpdatedAt: now,
	}
}

// snapshotAndPersist is a helper that takes a snapshot and persists it
// to a mock sink, returning the raw bytes.
func snapshotAndPersist(t *testing.T, m *MetadataFSM) []byte {
	t.Helper()

	snap, err := m.Snapshot()
	require.NoError(t, err)

	sink := &mockSnapshotSink{}
	err = snap.Persist(sink)
	require.NoError(t, err)
	require.True(t, sink.closed, "sink must be closed on success")
	require.False(t, sink.cancelled, "sink must not be cancelled on success")

	return sink.buf.Bytes()
}

// TestSnapshot_RoundTrip verifies the full Snapshot → Persist → Restore
// cycle: a populated FSM is snapshotted, persisted to bytes, and then
// restored onto a fresh FSM. Every registry entry must survive the trip.
func TestSnapshot_RoundTrip(t *testing.T) {
	original := newTestFSM(t)
	seedFullFSM(t, original)

	data := snapshotAndPersist(t, original)

	// Restore onto a fresh FSM.
	restored := newTestFSM(t)
	err := restored.Restore(io.NopCloser(bytes.NewReader(data)))
	require.NoError(t, err)

	// Nodes
	for id, want := range original.NodeRegistry {
		got, err := restored.GetNode(id)
		require.NoError(t, err, "node %s missing", id)
		assert.Equal(t, want.NodeID, got.NodeID)
		assert.Equal(t, want.Address, got.Address)
		assert.Equal(t, want.Status, got.Status)
		assert.Equal(t, want.FreeSpace, got.FreeSpace)
		assert.Equal(t, want.ChunkCount, got.ChunkCount)
	}

	// Files
	for id, want := range original.FileIndex {
		got, err := restored.GetFile(id)
		require.NoError(t, err, "file %s missing", id)
		assert.Equal(t, want.FileID, got.FileID)
		assert.Equal(t, want.Filename, got.Filename)
		assert.Equal(t, want.FileSize, got.FileSize)
		assert.Equal(t, want.Status, got.Status)
		assert.Equal(t, want.ChunkIDs, got.ChunkIDs)
		assert.Equal(t, want.CheckSum, got.CheckSum)
	}

	// Chunks
	for id, want := range original.ChunkRegistry {
		got, err := restored.GetChunk(id)
		require.NoError(t, err, "chunk %s missing", id)
		assert.Equal(t, want.ChunkID, got.ChunkID)
		assert.Equal(t, want.FileID, got.FileID)
		assert.Equal(t, want.ChunkIndex, got.ChunkIndex)
		assert.Equal(t, want.Size, got.Size)
		assert.Equal(t, want.Status, got.Status)
		assert.Equal(t, want.Checksum, got.Checksum)
		assert.Equal(t, want.Replicas, got.Replicas)
	}

	// Repair jobs
	for id, want := range original.RepairJobRegistry {
		got, err := restored.GetRepairJob(id)
		require.NoError(t, err, "job %s missing", id)
		assert.Equal(t, want.JobID, got.JobID)
		assert.Equal(t, want.ChunkID, got.ChunkID)
		assert.Equal(t, want.SourceNodeID, got.SourceNodeID)
		assert.Equal(t, want.TargetNodeID, got.TargetNodeID)
		assert.Equal(t, want.Status, got.Status)
		assert.Equal(t, want.Attempts, got.Attempts)
		assert.Equal(t, want.Error, got.Error)
	}
}

// TestSnapshot_EmptyFSM verifies that an empty FSM can be snapshotted
// and restored without error.
func TestSnapshot_EmptyFSM(t *testing.T) {
	original := newTestFSM(t)

	data := snapshotAndPersist(t, original)

	restored := newTestFSM(t)
	err := restored.Restore(io.NopCloser(bytes.NewReader(data)))
	require.NoError(t, err)

	assert.Empty(t, restored.FileIndex)
	assert.Empty(t, restored.ChunkRegistry)
	assert.Empty(t, restored.NodeRegistry)
	assert.Empty(t, restored.RepairJobRegistry)
}

// TestSnapshot_OverwritesExistingState ensures that Restore fully
// replaces whatever state was present before.
func TestSnapshot_OverwritesExistingState(t *testing.T) {
	// Build a source FSM with known state.
	source := newTestFSM(t)
	seedNode(t, source, "n-source", "10.0.0.1:1234")

	data := snapshotAndPersist(t, source)

	// Build a target FSM with different, pre-existing state.
	target := newTestFSM(t)
	seedNode(t, target, "n-old", "10.0.0.99:1234")
	seedFile(t, target, "old-file", "old.txt", []string{"old-ck"})
	seedRepairJob(t, target, "old-job")

	err := target.Restore(io.NopCloser(bytes.NewReader(data)))
	require.NoError(t, err)

	// Pre-existing state must be gone.
	_, err = target.GetNode("n-old")
	assert.ErrorIs(t, err, ErrNodeNotFound)
	_, err = target.GetFile("old-file")
	assert.ErrorIs(t, err, ErrFileNotFound)
	_, err = target.GetChunk("old-ck")
	assert.ErrorIs(t, err, ErrChunkNotFound)
	_, err = target.GetRepairJob("old-job")
	assert.ErrorIs(t, err, ErrJobNotFound)

	// Source state must be present.
	node, err := target.GetNode("n-source")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:1234", node.Address)
}

// TestSnapshot_Isolation verifies that mutations to the original FSM
// after taking a snapshot do NOT affect the snapshot data.
func TestSnapshot_Isolation(t *testing.T) {
	original := newTestFSM(t)
	seedNode(t, original, "node-iso", "10.0.0.5:9000")

	snap, err := original.Snapshot()
	require.NoError(t, err)

	// Mutate original AFTER snapshot was taken.
	original.NodeRegistry["node-iso"].Address = "MUTATED"
	seedNode(t, original, "node-extra", "10.0.0.6:9000")

	sink := &mockSnapshotSink{}
	err = snap.Persist(sink)
	require.NoError(t, err)

	restored := newTestFSM(t)
	err = restored.Restore(io.NopCloser(bytes.NewReader(sink.buf.Bytes())))
	require.NoError(t, err)

	// The restored FSM should have the original address, not the mutated one.
	node, err := restored.GetNode("node-iso")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.5:9000", node.Address,
		"snapshot must be isolated from post-snapshot mutations")

	// node-extra was added after the snapshot — it must not exist.
	_, err = restored.GetNode("node-extra")
	assert.ErrorIs(t, err, ErrNodeNotFound)
}

// TestRestore_CorruptData exercises the error path when the snapshot
// body is not valid JSON.
func TestRestore_CorruptData(t *testing.T) {
	m := newTestFSM(t)
	err := m.Restore(io.NopCloser(bytes.NewReader([]byte("not-valid-json"))))
	require.Error(t, err)

	// The FSM should remain empty (or at worst unchanged).
	assert.Empty(t, m.FileIndex)
	assert.Empty(t, m.NodeRegistry)
}

// TestRestore_EmptyReader exercises the error path when the snapshot
// body is empty (zero bytes).
func TestRestore_EmptyReader(t *testing.T) {
	m := newTestFSM(t)
	err := m.Restore(io.NopCloser(bytes.NewReader(nil)))
	require.Error(t, err, "restoring empty data must fail")
}

// TestPersist_FailingSink verifies that Persist cancels the sink when
// Write returns an error.
func TestPersist_FailingSink(t *testing.T) {
	original := newTestFSM(t)
	seedFullFSM(t, original)

	snap, err := original.Snapshot()
	require.NoError(t, err)

	sink := &failingSink{}
	err = snap.Persist(sink)
	require.Error(t, err)
	assert.True(t, sink.cancelled, "sink must be cancelled on write error")
}

// TestPersist_Release is a sanity check that Release does not panic.
func TestPersist_Release(t *testing.T) {
	snap := &MetadataFSMSnapshot{}
	require.NotPanics(t, func() { snap.Release() })
}

// TestRestoreFSM_Standalone tests RestoreFSM in isolation, without the
// full Persist/Restore pipeline.
func TestRestoreFSM_Standalone(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	logger := logging.NewCLogger()

	snap := &MetadataFSMSnapshot{
		FileIndex: map[string]FileRecord{
			"f1": {
				FileID: "f1", Filename: "readme.md", FileSize: 42,
				ChunkIDs: []string{"c1"}, Status: FileStatusComplete,
				CreatedAt: now,
			},
		},
		ChunkRegistry: map[string]ChunkRecord{
			"c1": {
				ChunkID: "c1", FileID: "f1", ChunkIndex: 0,
				Replicas: []string{"n1"}, Status: ChunkStatusComplete,
			},
		},
		NodeRegistry: map[string]NodeEntry{
			"n1": {
				NodeID: "n1", Address: "127.0.0.1:7000",
				Status: NodeStatusAlive, FreeSpace: 999,
			},
		},
		RepairJobRegistry: map[string]RepairJob{
			"j1": {
				JobID: "j1", Status: RepairStatusPending,
				CreatedAt: now, UpdatedAt: now,
			},
		},
	}

	fsm := snap.RestoreFSM(logger)
	require.NotNil(t, fsm)

	// Verify each entry was reconstructed as a pointer.
	file, err := fsm.GetFile("f1")
	require.NoError(t, err)
	assert.Equal(t, "readme.md", file.Filename)
	assert.Equal(t, uint64(42), file.FileSize)

	chunk, err := fsm.GetChunk("c1")
	require.NoError(t, err)
	assert.Equal(t, "f1", chunk.FileID)
	assert.Equal(t, []string{"n1"}, chunk.Replicas)

	node, err := fsm.GetNode("n1")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:7000", node.Address)

	job, err := fsm.GetRepairJob("j1")
	require.NoError(t, err)
	assert.Equal(t, RepairStatusPending, job.Status)
}

// TestRestoreFSM_ValueIsolation verifies that the pointer maps in the
// FSM returned by RestoreFSM do not share memory with the snapshot.
func TestRestoreFSM_ValueIsolation(t *testing.T) {
	logger := logging.NewCLogger()
	snap := &MetadataFSMSnapshot{
		NodeRegistry: map[string]NodeEntry{
			"n1": {NodeID: "n1", Address: "original"},
		},
		FileIndex:         map[string]FileRecord{},
		ChunkRegistry:     map[string]ChunkRecord{},
		RepairJobRegistry: map[string]RepairJob{},
	}

	fsm := snap.RestoreFSM(logger)

	// Mutate the snapshot after RestoreFSM.
	snap.NodeRegistry["n1"] = NodeEntry{NodeID: "n1", Address: "tampered"}

	node, err := fsm.GetNode("n1")
	require.NoError(t, err)
	assert.Equal(t, "original", node.Address,
		"RestoreFSM result must be isolated from snapshot mutations")
}

// TestSnapshot_PersistProducesValidJSON verifies that Persist writes
// well-formed JSON that can be independently unmarshalled.
func TestSnapshot_PersistProducesValidJSON(t *testing.T) {
	original := newTestFSM(t)
	seedFullFSM(t, original)

	data := snapshotAndPersist(t, original)

	var raw map[string]json.RawMessage
	err := json.Unmarshal(data, &raw)
	require.NoError(t, err, "Persist output must be valid JSON")

	// The top-level keys must match the struct's JSON tags.
	expectedKeys := []string{
		"file_index", "chunk_registry", "node_registry", "repairjob_registry",
	}
	for _, key := range expectedKeys {
		_, ok := raw[key]
		assert.True(t, ok, "missing top-level key %q", key)
	}
}

// TestSnapshot_MultipleRoundTrips does several consecutive snapshot →
// restore cycles to ensure repeatability.
func TestSnapshot_MultipleRoundTrips(t *testing.T) {
	m := newTestFSM(t)
	seedFullFSM(t, m)

	for i := 0; i < 3; i++ {
		data := snapshotAndPersist(t, m)

		fresh := newTestFSM(t)
		err := fresh.Restore(io.NopCloser(bytes.NewReader(data)))
		require.NoError(t, err, "round-trip %d failed", i)

		// Use the restored FSM as the source for the next cycle.
		m = fresh
	}

	// After 3 round-trips, all data must still be intact.
	node, err := m.GetNode("node-1")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:9000", node.Address)

	file, err := m.GetFile("file-alpha")
	require.NoError(t, err)
	assert.Equal(t, "alpha.txt", file.Filename)
}

// TestSnapshot_RestoreApplyInteraction verifies that a restored FSM
// can process new Apply() commands normally.
func TestSnapshot_RestoreApplyInteraction(t *testing.T) {
	original := newTestFSM(t)
	seedNode(t, original, "n1", "10.0.0.1:9000")

	data := snapshotAndPersist(t, original)

	restored := newTestFSM(t)
	err := restored.Restore(io.NopCloser(bytes.NewReader(data)))
	require.NoError(t, err)

	// Apply a MarkNodeDead command on the restored FSM.
	log := makeRaftLog(t, CmdMarkNodeDead, CommandMarkNodeDead{
		NodeID:    "n1",
		UpdatedAt: time.Now(),
	})
	result := restored.Apply(log)
	assert.Nil(t, result, "Apply on restored FSM should succeed")

	node, err := restored.GetNode("n1")
	require.NoError(t, err)
	assert.Equal(t, NodeStatusDead, node.Status)
}

// TestSnapshot_RegistryCounts verifies that the snapshot preserves the
// exact number of entries across all registries.
func TestSnapshot_RegistryCounts(t *testing.T) {
	original := newTestFSM(t)
	seedFullFSM(t, original)

	data := snapshotAndPersist(t, original)

	restored := newTestFSM(t)
	err := restored.Restore(io.NopCloser(bytes.NewReader(data)))
	require.NoError(t, err)

	assert.Equal(t, len(original.FileIndex), len(restored.FileIndex))
	assert.Equal(t, len(original.ChunkRegistry), len(restored.ChunkRegistry))
	assert.Equal(t, len(original.NodeRegistry), len(restored.NodeRegistry))
	assert.Equal(t, len(original.RepairJobRegistry), len(restored.RepairJobRegistry))
}
