package server

import (
	"testing"
	"time"

	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/reconcile"
	"github.com/satyam709/distributed-fs/metadata/scheduler"
	"github.com/satyam709/distributed-fs/metadata/watcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetChunkLocations_ReturnsLiveNodes(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-a", Address: "10.0.0.1:4000", FreeSpace: 1024, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-b", Address: "10.0.0.2:4000", FreeSpace: 2048, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-c", Address: "10.0.0.3:4000", FreeSpace: 512, CreatedAt: now,
	})

	applyFSMCommand(t, m, fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID:    "file-1",
		FileName:  "f.bin",
		ChunkIDs:  []string{"chunk-1"},
		FileSize:  1,
		CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID:  "chunk-1",
		NodeIDs:  []string{"node-a", "node-b", "node-c"},
		Checksum: []byte("sum"),
	})

	// Mark node-c dead — it should be filtered out.
	applyFSMCommand(t, m, fsm.CmdMarkNodeDead, fsm.CommandMarkNodeDead{
		NodeID: "node-c", UpdatedAt: now,
	})

	// GetChunkLocations is a read-only handler; we can call it without a
	// real Raft leader by using a handler with a nil raft field. The
	// isLeader check will panic on nil, so we short-circuit by constructing
	// the response directly through the FSM read method.
	loc, err := m.GetChunkLocations("chunk-1")
	require.NoError(t, err)
	require.Len(t, loc, 2)
	assert.Equal(t, "node-a", loc[0].NodeID)
	assert.Equal(t, "node-b", loc[1].NodeID)
}

func TestGetChunkLocations_ChunkNotFound(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	_, err := m.GetChunkLocations("ghost-chunk")
	require.Error(t, err)
	assert.ErrorIs(t, err, fsm.ErrChunkNotFound)
}

// Test that the handler constructor accepts the reconciler without panic.
func TestNewMetadataServiceHandler_WithReconciler(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	rec, err := reconcile.NewReconciler(m, nil, nil, nil, 0, 3)
	require.NoError(t, err)

	// Use a real RepairScheduler with a no-op proposer so the type matches.
	rs := scheduler.NewRepairScheduler(nil, m, nil, nil, 3)
	nw := watcher.NewNodeWatcher(m, nil, nil, 0, 0)

	deps := &HandlerDeps{
		FSM:               m,
		Scheduler:         rs,
		Watcher:           nw,
		Reconciler:        rec,
		ReplicationFactor: 3,
	}
	// This will panic if the constructor signature is wrong.
	h := NewMetadataServiceHandler(deps)
	require.NotNil(t, h)
}
