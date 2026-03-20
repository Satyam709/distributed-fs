package metadata

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- port allocator ---

var testPort atomic.Int32

func init() {
	testPort.Store(17100)
}

func nextAddr() string {
	port := testPort.Add(1)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// ---: minimal raft.FSM implementation for tests ---

// type struct{}

// func (f *testFSM()) Apply(log *raft.Log) interface{}        { return nil }
// func (f *testFSM) Snapshot() (raft.FSMSnapshot, error)   { return &testSnapshot{}, nil }
// func (f *testFSM) Restore(rc io.ReadCloser) error        { return rc.Close() }

// type testSnapshot struct{}

// func (s *testSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }
// func (s *testSnapshot) Release()                              {}

func newTestFSM() *fsm.MetadataFSM {
	return fsm.NewEmptyMetadataFsm(nil)
}

// --- helpers ---

func makeNodeConfig(dir, addr string) NodeConfig {
	return NodeConfig{
		NodeID:            "node1",
		RaftAddr:          addr,
		RaftDir:           dir,
		Bootstrap:         true,
		HeartbeatTimeout:  500 * time.Millisecond,
		ElectionTimeout:   500 * time.Millisecond,
		SnapshotInterval:  10 * time.Second,
		SnapshotThreshold: 1024,
		SnapshotRetain:    1,
	}
}

func setupSingleNodeRaft(t *testing.T) *raft.Raft {
	t.Helper()
	dir := t.TempDir()

	logStore, err := store.NewBoltStore(filepath.Join(dir, "raft.db"))
	require.NoError(t, err)

	cfg := RaftConfig{
		Config:      makeNodeConfig(dir, nextAddr()),
		FSM:         newTestFSM(),
		LogStore:    logStore,
		StableStore: logStore,
	}

	node, err := NewRaftNode(cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		node.Shutdown()
		err = logStore.Close()
		if err != nil {
			fmt.Println("err in setupSingleNodeRaft : ", err)
		}
	})

	return node
}

// setupRaftWithoutBootstrap creates a Raft node that intentionally never
// bootstraps — used to test WaitForLeader timeout behaviour.
func setupRaftWithoutBootstrap(t *testing.T) *raft.Raft {
	t.Helper()
	dir := t.TempDir()

	logStore, err := store.NewBoltStore(filepath.Join(dir, "raft.db"))
	require.NoError(t, err)

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = "node-noboot"
	raftCfg.HeartbeatTimeout = 500 * time.Millisecond
	raftCfg.ElectionTimeout = 500 * time.Millisecond

	snapshotStore, err := raft.NewFileSnapshotStore(dir, 1, nil)
	require.NoError(t, err)

	transport, err := raft.NewTCPTransport(nextAddr(), nil, 3, 5*time.Second, nil)
	require.NoError(t, err)

	node, err := raft.NewRaft(raftCfg, newTestFSM(), logStore, logStore, snapshotStore, transport)
	require.NoError(t, err)

	// deliberately NOT calling BootstrapCluster — node will never get a leader

	t.Cleanup(func() {
		node.Shutdown()
		err = logStore.Close()
		if err != nil {
			fmt.Println("err in setupSingleNodeRaft : ", err)
		}
	})

	return node
}

// --- tests ---

func TestIsLeader(t *testing.T) {
	r := setupSingleNodeRaft(t)

	err := WaitForLeader(r, 5*time.Second)
	require.NoError(t, err)

	assert.True(t, IsLeader(r))
}

func TestLeaderAddress(t *testing.T) {
	r := setupSingleNodeRaft(t)

	err := WaitForLeader(r, 5*time.Second)
	require.NoError(t, err)

	addr := LeaderAddress(r)
	assert.NotEmpty(t, addr)
}

func TestWaitForLeader_Success(t *testing.T) {
	r := setupSingleNodeRaft(t)

	err := WaitForLeader(r, 5*time.Second)

	assert.NoError(t, err)
}

func TestWaitForLeader_Timeout(t *testing.T) {
	r := setupRaftWithoutBootstrap(t)

	err := WaitForLeader(r, 1*time.Second)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestNewRaftNode_Success(t *testing.T) {
	dir := t.TempDir()

	logStore, err := store.NewBoltStore(filepath.Join(dir, "raft.db"))
	require.NoError(t, err)

	cfg := RaftConfig{
		Config:      makeNodeConfig(dir, nextAddr()),
		FSM:         newTestFSM(),
		LogStore:    logStore,
		StableStore: logStore,
	}

	node, err := NewRaftNode(cfg)
	require.NoError(t, err)
	assert.NotNil(t, node)

	t.Cleanup(func() {
		node.Shutdown()
		err = logStore.Close()
		if err != nil {
			fmt.Println("err in setupSingleNodeRaft : ", err)
		}
	})
}

func TestNewRaftNode_InvalidAddr(t *testing.T) {
	dir := t.TempDir()

	logStore, err := store.NewBoltStore(filepath.Join(dir, "raft.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = logStore.Close()
		if err != nil {
			fmt.Println("err in setupSingleNodeRaft : ", err)
		}
	}) // registered immediately — runs regardless of what happens next

	cfg := RaftConfig{
		Config:      makeNodeConfig(dir, "not-a-valid-addr"),
		FSM:         newTestFSM(),
		LogStore:    logStore,
		StableStore: logStore,
	}

	node, err := NewRaftNode(cfg)
	assert.Error(t, err)
	assert.Nil(t, node)
}

func TestBootstrap_SkippedOnRestart(t *testing.T) {
	dir := t.TempDir()
	addr := nextAddr()

	newStore := func() *raftboltdb.BoltStore {
		db, err := store.NewBoltStore(filepath.Join(dir, "raft.db"))
		require.NoError(t, err)
		return db
	}

	// --- first startup: bootstrap should run ---
	store1 := newStore()
	node1, err := NewRaftNode(RaftConfig{
		Config:      makeNodeConfig(dir, addr),
		FSM:         newTestFSM(),
		LogStore:    store1,
		StableStore: store1,
	})
	require.NoError(t, err)

	err = WaitForLeader(node1, 5*time.Second)
	require.NoError(t, err)

	// block until fully shut down — releases TCP port
	if err := node1.Shutdown().Error(); err != nil {
		t.Logf("node1 shutdown error: %v", err)
	}
	err = store1.Close()
	if err != nil {
			fmt.Println("err in setupSingleNodeRaft : ", err)
		}

	restartCfg := makeNodeConfig(dir, addr)
	restartCfg.Bootstrap = false // simulates operator forgetting to set Bootstrap=true on restart

	// --- second startup: same dir, HasExistingState = true, bootstrap skipped ---
	var node2 *raft.Raft
	var store2 *raftboltdb.BoltStore
	for i := 0; i < 10; i++ {
		store2 = newStore()
		node2, err = NewRaftNode(RaftConfig{
			Config:      restartCfg,
			FSM:         newTestFSM(),
			LogStore:    store2,
			StableStore: store2,
		})
		if err == nil {
			break
		}
		err = store2.Close()
		if err != nil {
			fmt.Println("err in setupSingleNodeRaft : ", err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	require.NoError(t, err)
	assert.NotNil(t, node2)
	t.Cleanup(func() {
		if err := node2.Shutdown().Error(); err != nil {
			t.Logf("node2 shutdown error: %v", err)
		}
		err = store2.Close()
		if err != nil {
			fmt.Println("err in setupSingleNodeRaft : ", err)
		}
	})
}

func TestLeaderAddress_Format(t *testing.T) {
	r := setupSingleNodeRaft(t)

	err := WaitForLeader(r, 5*time.Second)
	require.NoError(t, err)

	addr := LeaderAddress(r)
	assert.Contains(t, addr, ":")
	assert.NotEqual(t, addr, ":")
}
