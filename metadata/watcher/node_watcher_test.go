package watcher

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProposer records all proposed commands and allows controlling
// leadership status.
type mockProposer struct {
	mu       sync.Mutex
	leader   bool
	proposed []fsm.MetadataCommand
	err      error // if non-nil, Propose returns this
}

func (m *mockProposer) IsLeader() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leader
}

func (m *mockProposer) Propose(cmd fsm.MetadataCommand) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.proposed = append(m.proposed, cmd)
	return nil
}

func (m *mockProposer) getProposed() []fsm.MetadataCommand {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]fsm.MetadataCommand, len(m.proposed))
	copy(cp, m.proposed)
	return cp
}

// mockRepairTriggerer records TriggerRepair calls.
type mockRepairTriggerer struct {
	mu        sync.Mutex
	triggered []string
}

func (m *mockRepairTriggerer) TriggerRepair(deadNodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.triggered = append(m.triggered, deadNodeID)
}

func (m *mockRepairTriggerer) getTriggered() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.triggered))
	copy(cp, m.triggered)
	return cp
}

// helpers

func newTestFSM(t *testing.T) *fsm.MetadataFSM {
	t.Helper()
	return fsm.NewEmptyMetadataFsm(logging.NewCLogger())
}

func addNodeToFsm(t *testing.T, mockFsm *fsm.MetadataFSM, nodes ...*fsm.NodeEntry) {
	for _, val := range nodes {
		addNodeCmd := fsm.CommandRegisterNode{
			NodeID:     val.NodeID,
			Address:    val.Address,
			FreeSpace:  val.FreeSpace,
			ChunkCount: val.ChunkCount,
			CreatedAt:  val.RegisteredAt,
		}
		data, err := json.Marshal(addNodeCmd)
		require.NoError(t, err, "failed to marshal add node")
		cmd := fsm.MetadataCommand{
			Type:    fsm.CmdRegisterNode,
			Payload: data,
		}
		data, err = json.Marshal(cmd)
		require.NoError(t, err, "failed to marshal add node")

		res := mockFsm.Apply(&raft.Log{
			Index: 1,
			Term:  1,
			Data:  data,
		})

		require.Nil(t, res, "res of addnode apply not nil")
	}
}

func markNodeDead(t *testing.T, mockFsm *fsm.MetadataFSM, nodeID string, updatedAt time.Time) {
	addNodeCmd := fsm.CommandMarkNodeDead{
		NodeID:    nodeID,
		UpdatedAt: updatedAt,
	}
	data, err := json.Marshal(addNodeCmd)
	require.NoError(t, err, "failed to marshal markdead node")
	cmd := fsm.MetadataCommand{
		Type:    fsm.CmdMarkNodeDead,
		Payload: data,
	}
	data, err = json.Marshal(cmd)
	require.NoError(t, err, "failed to marshal markdead node")

	res := mockFsm.Apply(&raft.Log{
		Index: 1,
		Term:  1,
		Data:  data,
	})

	require.Nil(t, res, "res of marknode-dead apply not nil")
}

func seedAliveNode(t *testing.T, m *fsm.MetadataFSM, nodeID string, lastSeen time.Time) {
	t.Helper()
	node := &fsm.NodeEntry{
		NodeID:       nodeID,
		Address:      nodeID + ":8000",
		Status:       fsm.NodeStatusAlive,
		FreeSpace:    1000,
		LastSeen:     lastSeen,
		RegisteredAt: lastSeen,
		UpdatedAt:    lastSeen,
	}
	addNodeToFsm(t, m, node)
	require.NoError(t, m.UpdateLastSeen(nodeID, lastSeen), "lastseen update failed")
}

func seedDeadNode(t *testing.T, m *fsm.MetadataFSM, nodeID string, lastSeen time.Time) {
	t.Helper()
	node := &fsm.NodeEntry{
		NodeID:       nodeID,
		Address:      nodeID + ":8000",
		Status:       fsm.NodeStatusDead,
		FreeSpace:    1000,
		LastSeen:     lastSeen,
		RegisteredAt: lastSeen,
		UpdatedAt:    lastSeen,
	}
	addNodeToFsm(t, m, node)
	require.NoError(t, m.UpdateLastSeen(nodeID, lastSeen), "lastseen update failed")

	// mark it dead
	markNodeDead(t, m, nodeID, lastSeen)
}

// Tests

func TestSweep_SkipsWhenNotLeader(t *testing.T) {
	f := newTestFSM(t)
	stale := time.Now().Add(-1 * time.Minute)
	seedAliveNode(t, f, "node-1", stale)

	proposer := &mockProposer{leader: false}
	repair := &mockRepairTriggerer{}

	nw := NewNodeWatcher(f, proposer, repair, 10*time.Second, 5*time.Second)
	nw.sweep()

	assert.Empty(t, proposer.getProposed(), "should not propose commands when not leader")
	assert.Empty(t, repair.getTriggered(), "should not trigger repair when not leader")
}

func TestSweep_MarksStaleNodeDead(t *testing.T) {
	f := newTestFSM(t)
	suspectTimeout := 10 * time.Second
	stale := time.Now().Add(-20 * time.Second) // well past timeout
	seedAliveNode(t, f, "node-stale", stale)

	proposer := &mockProposer{leader: true}
	repair := &mockRepairTriggerer{}

	nw := NewNodeWatcher(f, proposer, repair, suspectTimeout, 5*time.Second)
	nw.sweep()

	proposed := proposer.getProposed()
	require.Len(t, proposed, 1, "should propose exactly one CmdMarkNodeDead")
	assert.Equal(t, fsm.CmdMarkNodeDead, proposed[0].Type)

	triggered := repair.getTriggered()
	require.Len(t, triggered, 1, "should trigger repair for the dead node")
	assert.Equal(t, "node-stale", triggered[0])
}

func TestSweep_IgnoresFreshNodes(t *testing.T) {
	f := newTestFSM(t)
	suspectTimeout := 10 * time.Second
	recent := time.Now().Add(-2 * time.Second) // well within timeout
	seedAliveNode(t, f, "node-fresh", recent)

	proposer := &mockProposer{leader: true}
	repair := &mockRepairTriggerer{}

	nw := NewNodeWatcher(f, proposer, repair, suspectTimeout, 5*time.Second)
	nw.sweep()

	assert.Empty(t, proposer.getProposed(), "should not propose for fresh nodes")
	assert.Empty(t, repair.getTriggered(), "should not trigger repair for fresh nodes")
}

func TestSweep_IgnoresAlreadyDeadNodes(t *testing.T) {
	f := newTestFSM(t)
	suspectTimeout := 10 * time.Second
	stale := time.Now().Add(-1 * time.Minute)
	seedDeadNode(t, f, "node-dead", stale)

	proposer := &mockProposer{leader: true}
	repair := &mockRepairTriggerer{}

	nw := NewNodeWatcher(f, proposer, repair, suspectTimeout, 5*time.Second)
	nw.sweep()

	assert.Empty(t, proposer.getProposed(), "should not re-propose for already dead nodes")
	assert.Empty(t, repair.getTriggered(), "should not trigger repair for already dead nodes")
}

func TestSweep_MultipleNodes(t *testing.T) {
	f := newTestFSM(t)
	suspectTimeout := 10 * time.Second

	seedAliveNode(t, f, "node-a", time.Now().Add(-20*time.Second)) // stale
	seedAliveNode(t, f, "node-b", time.Now().Add(-2*time.Second))  // fresh
	seedAliveNode(t, f, "node-c", time.Now().Add(-30*time.Second)) // stale
	seedDeadNode(t, f, "node-d", time.Now().Add(-1*time.Minute))   // already dead

	proposer := &mockProposer{leader: true}
	repair := &mockRepairTriggerer{}

	nw := NewNodeWatcher(f, proposer, repair, suspectTimeout, 5*time.Second)
	nw.sweep()

	proposed := proposer.getProposed()
	assert.Len(t, proposed, 2, "should propose CmdMarkNodeDead for two stale nodes")
	for _, cmd := range proposed {
		assert.Equal(t, fsm.CmdMarkNodeDead, cmd.Type)
	}

	triggered := repair.getTriggered()
	assert.Len(t, triggered, 2, "should trigger repair for two dead nodes")
}

func TestUpdateLastSeen(t *testing.T) {
	f := newTestFSM(t)
	oldTime := time.Now().Add(-1 * time.Hour)
	seedAliveNode(t, f, "node-1", oldTime)

	newTime := time.Now()
	err := f.UpdateLastSeen("node-1", newTime)
	require.NoError(t, err)

	node, err := f.GetNode("node-1")
	require.NoError(t, err)
	assert.Equal(t, newTime, node.LastSeen, "LastSeen should be updated")
}

func TestUpdateLastSeen_NodeNotFound(t *testing.T) {
	f := newTestFSM(t)
	err := f.UpdateLastSeen("nonexistent", time.Now())
	assert.ErrorIs(t, err, fsm.ErrNodeNotFound)
}

func TestGetAllNodes(t *testing.T) {
	f := newTestFSM(t)
	now := time.Now()
	seedAliveNode(t, f, "node-1", now)
	seedAliveNode(t, f, "node-2", now)
	seedDeadNode(t, f, "node-3", now)

	all := f.GetAllNodes()
	assert.Len(t, all, 3, "should return all nodes regardless of status")
}

func TestStartAndStop(t *testing.T) {
	f := newTestFSM(t)
	proposer := &mockProposer{leader: false}
	repair := &mockRepairTriggerer{}

	nw := NewNodeWatcher(f, proposer, repair, 10*time.Second, 50*time.Millisecond)

	done := make(chan struct{})
	go func() {
		nw.Start()
		close(done)
	}()

	// Let the ticker fire a couple of times.
	time.Sleep(150 * time.Millisecond)

	nw.Stop()

	select {
	case <-done:
		// goroutine exited cleanly
	case <-time.After(2 * time.Second):
		t.Fatal("NodeWatcher did not stop within timeout")
	}
}
