package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/placement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// testProposer records every proposed command and applies it to a real FSM
// so subsequent reads return consistent state.
type testProposer struct {
	fsm      *fsm.MetadataFSM
	proposed []fsm.MetadataCommand
}

func (tp *testProposer) propose(cmd fsm.MetadataCommand) error {
	tp.proposed = append(tp.proposed, cmd)
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

// placementStub returns pre-configured nodes.
type placementStub struct {
	nodes        []fsm.NodeEntry
	reverseNodes []fsm.NodeEntry
	reverseCount int
}

func (p *placementStub) SelectNodes(_ []fsm.NodeEntry, _ string, _ int, count int, _ ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	if count > len(p.nodes) {
		return p.nodes, nil
	}
	return p.nodes[:count], nil
}
func (p *placementStub) SelectNodeReverse(_ []fsm.NodeEntry, _ string, _ int, count int, _ ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	p.reverseCount = count
	if count > len(p.reverseNodes) {
		return p.reverseNodes, nil
	}
	return p.reverseNodes[:count], nil
}
func (p *placementStub) SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry {
	if len(nodes) == 0 {
		return fsm.NodeEntry{}
	}
	return nodes[0]
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

// setupBaseState registers nodes, creates a file, and commits chunks to
// give the scheduler something to work with.
func setupBaseState(t *testing.T, m *fsm.MetadataFSM) {
	t.Helper()
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-a", Address: "10.0.0.1:4000", FreeSpace: 1024, ChunkCount: 5, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-b", Address: "10.0.0.2:4000", FreeSpace: 2048, ChunkCount: 2, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-c", Address: "10.0.0.3:4000", FreeSpace: 512, ChunkCount: 10, CreatedAt: now,
	})

	applyFSMCommand(t, m, fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID: "file-1", FileName: "f.bin", ChunkIDs: []string{"chunk-1", "chunk-2"}, FileSize: 2,
		CreatedAt: now,
	})

	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID: "chunk-1", NodeIDs: []string{"node-a", "node-b"}, Checksum: []byte("sum-1"),
	})
	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID: "chunk-2", NodeIDs: []string{"node-a", "node-b"}, Checksum: []byte("sum-2"),
	})
}

func newTestScheduler(t *testing.T, m *fsm.MetadataFSM, ps *placementStub, rf int) (*RepairScheduler, *testProposer) {
	t.Helper()
	tp := &testProposer{fsm: m}
	rs := newTestableScheduler(m, placement.LeastLoadedStrategy{}, ps, rf, tp.propose)
	return rs, tp
}

// ---------------------------------------------------------------------------
// Tests: scheduleRepairJobs
// ---------------------------------------------------------------------------

func TestScheduleRepairJobs_OverReplicatedUsesPositiveReverseCount(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	ps := &placementStub{}
	rs, _ := newTestScheduler(t, m, ps, 3)

	rs.scheduleRepairJobs("chunk-1", nil, -2)

	assert.Equal(t, 2, ps.reverseCount)
}

func TestScheduleRepairJobs_UnderReplicated_CreatesJobs(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)

	target := fsm.NodeEntry{NodeID: "node-c", FreeSpace: 512}
	ps := &placementStub{nodes: []fsm.NodeEntry{target}}
	rs, tp := newTestScheduler(t, m, ps, 3)

	liveReplicas := []fsm.NodeEntry{
		{NodeID: "node-a", ChunkCount: 5},
		{NodeID: "node-b", ChunkCount: 2},
	}
	rs.scheduleRepairJobs("chunk-1", liveReplicas, 1)

	// Should have proposed exactly one CmdCreateRepairJob
	require.Len(t, tp.proposed, 1)
	assert.Equal(t, fsm.CmdCreateRepairJob, tp.proposed[0].Type)

	// The job should be in the jobs channel
	require.Len(t, rs.jobs, 1)

	// Verify the job was created with the least-loaded source (node-b has ChunkCount=2)
	var cmd fsm.CommandCreateRepairJob
	require.NoError(t, json.Unmarshal(tp.proposed[0].Payload, &cmd))
	assert.Equal(t, "node-b", cmd.SourceNode, "should pick least-loaded node as source")
	assert.Equal(t, "node-c", cmd.TargetNode)
	assert.Equal(t, "chunk-1", cmd.ChunkID)
	assert.False(t, cmd.DeleteSource)
}

func TestScheduleRepairJobs_NoLiveReplicas_Skips(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	ps := &placementStub{}
	rs, tp := newTestScheduler(t, m, ps, 3)

	rs.scheduleRepairJobs("chunk-1", nil, 1)

	assert.Empty(t, tp.proposed, "should not propose anything without live replicas")
	assert.Len(t, rs.jobs, 0)
}

func TestScheduleRepairJobs_NonBlockingSend(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)

	// Create many target nodes
	var targets []fsm.NodeEntry
	for i := 0; i < repairQueueCapacity+10; i++ {
		targets = append(targets, fsm.NodeEntry{NodeID: "target-node"})
	}
	ps := &placementStub{nodes: targets}
	rs, _ := newTestScheduler(t, m, ps, repairQueueCapacity+10+2)

	liveReplicas := []fsm.NodeEntry{{NodeID: "node-a", ChunkCount: 1}}

	// This should NOT deadlock even though we're sending more than buffer size
	done := make(chan struct{})
	go func() {
		rs.scheduleRepairJobs("chunk-1", liveReplicas, repairQueueCapacity+10)
		close(done)
	}()

	select {
	case <-done:
		// success - didn't block
	case <-time.After(2 * time.Second):
		t.Fatal("scheduleRepairJobs blocked — channel deadlock not fixed")
	}
}

// ---------------------------------------------------------------------------
// Tests: ScheduleRepairForChunk
// ---------------------------------------------------------------------------

func TestScheduleRepairForChunk_UnderReplicated_CreatesJobs(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)

	// RF=3, chunks only have 2 replicas (node-a, node-b) => deficit=1
	target := fsm.NodeEntry{NodeID: "node-c", FreeSpace: 512}
	ps := &placementStub{nodes: []fsm.NodeEntry{target}}
	rs, tp := newTestScheduler(t, m, ps, 3)

	rs.ScheduleRepairForChunk("chunk-1")

	require.Len(t, tp.proposed, 1)
	assert.Equal(t, fsm.CmdCreateRepairJob, tp.proposed[0].Type)
	assert.Len(t, rs.jobs, 1)
}

func TestScheduleRepairForChunk_FullyReplicated_NoOp(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)

	// RF=2, chunks have exactly 2 replicas
	ps := &placementStub{}
	rs, tp := newTestScheduler(t, m, ps, 2)

	rs.ScheduleRepairForChunk("chunk-1")

	assert.Empty(t, tp.proposed)
	assert.Len(t, rs.jobs, 0)
}

func TestScheduleRepairForChunk_ChunkNotFound(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	ps := &placementStub{}
	rs, tp := newTestScheduler(t, m, ps, 3)

	rs.ScheduleRepairForChunk("ghost-chunk")

	assert.Empty(t, tp.proposed)
	assert.Len(t, rs.jobs, 0)
}

// ---------------------------------------------------------------------------
// Tests: TriggerRepair
// ---------------------------------------------------------------------------

func TestTriggerRepair_UnderReplicated(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)

	// Mark node-a as dead so chunk-1 and chunk-2 only have 1 replica each
	applyFSMCommand(t, m, fsm.CmdMarkNodeDead, fsm.CommandMarkNodeDead{
		NodeID: "node-a", UpdatedAt: time.Now(),
	})

	target := fsm.NodeEntry{NodeID: "node-c", FreeSpace: 512}
	ps := &placementStub{nodes: []fsm.NodeEntry{target}}
	rs, tp := newTestScheduler(t, m, ps, 2) // RF=2, need 1 more per chunk

	rs.TriggerRepair("node-a")

	// Should create repair jobs for both chunks
	var createCommands int
	for _, cmd := range tp.proposed {
		if cmd.Type == fsm.CmdCreateRepairJob {
			createCommands++
		}
	}
	assert.Equal(t, 2, createCommands, "should create one repair job per under-replicated chunk")
}

func TestTriggerRepair_FullyReplicated(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)

	// RF=2, chunks have 2 replicas (node-a, node-b) — fully replicated
	ps := &placementStub{}
	rs, tp := newTestScheduler(t, m, ps, 2)

	// node-c is not holding any chunks so trigger shouldn't do anything meaningful
	rs.TriggerRepair("node-c")

	assert.Empty(t, tp.proposed, "should not create jobs when chunks are fully replicated")
}

// ---------------------------------------------------------------------------
// Tests: executeJob
// ---------------------------------------------------------------------------

func TestExecuteJob_AddsToPendingAndMarksInProgress(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
		JobID: "job-1", CreatedAt: now, ChunkID: "chunk-1",
		SourceNode: "node-a", TargetNode: "node-b",
	})

	rs, tp := newTestScheduler(t, m, &placementStub{}, 3)
	rs.executeJob("job-1")

	// Check pending jobs
	pending := rs.GetPendingJobsForNode("node-a")
	assert.Contains(t, pending, "job-1")

	// Check that CmdUpdateRepairJob was proposed
	require.Len(t, tp.proposed, 1)
	assert.Equal(t, fsm.CmdUpdateRepairJob, tp.proposed[0].Type)

	// Verify job is now in-progress in FSM
	job, err := m.GetRepairJob("job-1")
	require.NoError(t, err)
	assert.Equal(t, fsm.RepairStatusInProgress, job.Status)
}

func TestDupsJobsRejection(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())

	setupBaseState(t, m)
	targetNode := "node-c"
	ps := &placementStub{[]fsm.NodeEntry{{
		NodeID:    targetNode,
		FreeSpace: 1024,
	}}, nil, 0}
	rs, _ := newTestScheduler(t, m, ps, 3)
	rs.ScheduleRepairForChunk("chunk-1")
	jobs, _ := m.GetJobsByStatus(fsm.RepairStatusPending)
	require.Len(t, jobs, 1, "job should be present")
	// executeJob
	rs.executeJob(<-rs.jobs)

	// try re-adding the same job
	rs.ScheduleRepairForChunk("chunk-1")
	rs.ScheduleRepairForChunk("chunk-1")
	rs.ScheduleRepairForChunk("chunk-1")

	select {
	case j := <-rs.jobs:
		assert.Empty(t, j, "found the dup job %v", j)
	default:
	}
}

// ---------------------------------------------------------------------------
// Tests: OnJobComplete
// ---------------------------------------------------------------------------

func TestOnJobComplete_Success_AddsReplica(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
		JobID: "job-1", CreatedAt: now, ChunkID: "chunk-1",
		SourceNode: "node-a", TargetNode: "node-c",
	})

	rs, tp := newTestScheduler(t, m, &placementStub{}, 3)
	// Simulate executeJob adding to pending map
	rs.mutex.Lock()
	rs.pendingJobs["node-a"] = []string{"job-1"}
	rs.mutex.Unlock()

	err := rs.OnJobComplete("node-a", "job-1", true, "")
	require.NoError(t, err)

	// Should have proposed CmdAddChunkReplica
	require.Len(t, tp.proposed, 1)
	assert.Equal(t, fsm.CmdAddChunkReplica, tp.proposed[0].Type)

	var addCmd fsm.CommandAddChunkReplica
	require.NoError(t, json.Unmarshal(tp.proposed[0].Payload, &addCmd))
	assert.Equal(t, "chunk-1", addCmd.ChunkID)
	assert.Equal(t, "node-c", addCmd.NodeID)

	// Verify chunk now has node-c in replicas
	locations, err := m.GetChunkLocations("chunk-1")
	require.NoError(t, err)
	nodeIDs := make([]string, len(locations))
	for i, n := range locations {
		nodeIDs[i] = n.NodeID
	}
	assert.Contains(t, nodeIDs, "node-c")

	// Should have removed from pending
	assert.Empty(t, rs.GetPendingJobsForNode("node-a"))
}

func TestOnJobComplete_Success_DeleteSource_EvictsReplica(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m)
	now := time.Now()

	// Over-replicated scenario: delete source node-a from chunk-1
	applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
		JobID: "job-del", CreatedAt: now, ChunkID: "chunk-1",
		DeleteSource: true, SourceNode: "node-a",
	})

	rs, tp := newTestScheduler(t, m, &placementStub{}, 3)
	rs.mutex.Lock()
	rs.pendingJobs["node-a"] = []string{"job-del"}
	rs.mutex.Unlock()

	err := rs.OnJobComplete("node-a", "job-del", true, "")
	require.NoError(t, err)

	// Should have proposed CmdEvictChunkFromNode
	require.Len(t, tp.proposed, 1)
	assert.Equal(t, fsm.CmdEvictChunkFromNode, tp.proposed[0].Type)

	var evictCmd fsm.CommandEvictChunkFromNode
	require.NoError(t, json.Unmarshal(tp.proposed[0].Payload, &evictCmd))
	assert.Equal(t, "chunk-1", evictCmd.ChunkID)
	assert.Equal(t, "node-a", evictCmd.NodeID)

	// Verify node-a is no longer in chunk-1's replicas
	locations, err := m.GetChunkLocations("chunk-1")
	require.NoError(t, err)
	for _, loc := range locations {
		assert.NotEqual(t, "node-a", loc.NodeID)
	}
}

func TestOnJobComplete_Failure_NoRescheduleWhenMet(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	setupBaseState(t, m) // chunks have 2 replicas

	now := time.Now()
	applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
		JobID: "job-ok", CreatedAt: now, ChunkID: "chunk-1",
		SourceNode: "node-a", TargetNode: "node-c",
	})
	applyFSMCommand(t, m, fsm.CmdUpdateRepairJob, fsm.CommandUpdateRepairJob{
		JobID: "job-ok", Status: fsm.RepairStatusFailed, UpdatedAt: now,
	})

	ps := &placementStub{}
	rs, tp := newTestScheduler(t, m, ps, 2) // RF=2, chunk has 2 replicas

	err := rs.OnJobComplete("node-a", "job-ok", false, "err")
	require.NoError(t, err)

	// No new jobs should be created
	for _, cmd := range tp.proposed {
		assert.NotEqual(t, fsm.CmdCreateRepairJob, cmd.Type, "should not reschedule when replication met")
	}
}

// ---------------------------------------------------------------------------
// Tests: RecoverStuckJobs
// ---------------------------------------------------------------------------

func TestRecoverStuckJobs_RequeuesAllAliveSourceJobs(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-a", Address: "10.0.0.1:4000", FreeSpace: 1024, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-b", Address: "10.0.0.2:4000", FreeSpace: 1024, CreatedAt: now,
	})

	applyFSMCommand(t, m, fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID: "file-1", FileName: "f.bin", ChunkIDs: []string{"chunk-1", "chunk-2"}, FileSize: 2,
		CreatedAt: now,
	})

	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID: "chunk-1", NodeIDs: []string{"node-a", "node-b"}, Checksum: []byte("sum-1"),
	})
	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID: "chunk-2", NodeIDs: []string{"node-a", "node-b"}, Checksum: []byte("sum-2"),
	})

	for _, jobID := range []string{"job-1", "job-2"} {
		chunkID := "chunk-1"
		if jobID == "job-2" {
			chunkID = "chunk-2"
		}

		applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
			JobID:      jobID,
			CreatedAt:  now,
			ChunkID:    chunkID,
			SourceNode: "node-a",
			TargetNode: "node-c",
		})
		applyFSMCommand(t, m, fsm.CmdUpdateRepairJob, fsm.CommandUpdateRepairJob{
			JobID: jobID, Status: fsm.RepairStatusInProgress, UpdatedAt: now, Attempts: 1,
		})
	}

	rs, _ := newTestScheduler(t, m, &placementStub{}, 3)
	rs.RecoverStuckJobs(context.Background())

	requeued := map[string]bool{}
	for len(rs.jobs) > 0 {
		requeued[<-rs.jobs] = true
	}

	assert.Len(t, requeued, 2)
	assert.True(t, requeued["job-1"])
	assert.True(t, requeued["job-2"])
}

func TestRecoverStuckJobs_SkipsFullyReplicated(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-a", Address: "10.0.0.1:4000", FreeSpace: 1024, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-b", Address: "10.0.0.2:4000", FreeSpace: 1024, CreatedAt: now,
	})

	applyFSMCommand(t, m, fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID: "file-1", FileName: "f.bin", ChunkIDs: []string{"chunk-1"}, FileSize: 1,
		CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID: "chunk-1", NodeIDs: []string{"node-a", "node-b"}, Checksum: []byte("sum-1"),
	})

	applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
		JobID: "stuck-job", CreatedAt: now, ChunkID: "chunk-1",
		SourceNode: "node-a", TargetNode: "node-c",
	})
	applyFSMCommand(t, m, fsm.CmdUpdateRepairJob, fsm.CommandUpdateRepairJob{
		JobID: "stuck-job", Status: fsm.RepairStatusInProgress, UpdatedAt: now,
	})

	rs, _ := newTestScheduler(t, m, &placementStub{}, 2) // RF=2, chunk has 2 replicas
	rs.RecoverStuckJobs(context.Background())

	assert.Len(t, rs.jobs, 0, "should skip fully-replicated chunks (deficit <= 0)")
}

func TestRecoverStuckJobs_RespectsContextCancellation(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-a", Address: "10.0.0.1:4000", FreeSpace: 1024, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID: "file-1", FileName: "f.bin", ChunkIDs: []string{"chunk-1"}, FileSize: 1,
		CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdCommitChunk, fsm.CommandCommitChunk{
		ChunkID: "chunk-1", NodeIDs: []string{"node-a"}, Checksum: []byte("sum"),
	})
	applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
		JobID: "job-1", CreatedAt: now, ChunkID: "chunk-1",
		SourceNode: "node-a", TargetNode: "node-b",
	})
	applyFSMCommand(t, m, fsm.CmdUpdateRepairJob, fsm.CommandUpdateRepairJob{
		JobID: "job-1", Status: fsm.RepairStatusInProgress, UpdatedAt: now,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	rs, _ := newTestScheduler(t, m, &placementStub{}, 3)
	rs.RecoverStuckJobs(ctx)

	assert.Len(t, rs.jobs, 0, "should not enqueue when context is cancelled")
}

// ---------------------------------------------------------------------------
// Tests: Stop (graceful shutdown)
// ---------------------------------------------------------------------------

func TestStop_GracefulShutdown(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	ps := &placementStub{}
	rs, _ := newTestScheduler(t, m, ps, 3)

	rs.Start(context.Background())

	done := make(chan struct{})
	go func() {
		rs.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(3 * time.Second):
		t.Fatal("Stop() did not return in time — workers not draining")
	}
}

// ---------------------------------------------------------------------------
// Tests: GetPendingJobsForNode
// ---------------------------------------------------------------------------

func TestGetPendingJobsForNode_ReturnsCopy(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	rs, _ := newTestScheduler(t, m, &placementStub{}, 3)

	rs.mutex.Lock()
	rs.pendingJobs["node-a"] = []string{"j1", "j2"}
	rs.mutex.Unlock()

	result := rs.GetPendingJobsForNode("node-a")
	require.Len(t, result, 2)

	// Mutating the returned slice should NOT affect internal state
	result[0] = "mutated"
	internal := rs.GetPendingJobsForNode("node-a")
	assert.Equal(t, "j1", internal[0], "should return a copy, not internal slice")
}

func TestGetPendingJobsForNode_UnknownNodeReturnsNil(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	rs, _ := newTestScheduler(t, m, &placementStub{}, 3)

	assert.Nil(t, rs.GetPendingJobsForNode("unknown"))
}

// ---------------------------------------------------------------------------
// Tests: LeastLoadedStrategy as source strategy
// ---------------------------------------------------------------------------

func TestSourceStrategy_SelectPrimary_PicksLeastLoaded(t *testing.T) {
	nodes := []fsm.NodeEntry{
		{NodeID: "heavy", ChunkCount: 100},
		{NodeID: "light", ChunkCount: 1},
		{NodeID: "medium", ChunkCount: 50},
	}
	strategy := placement.LeastLoadedStrategy{}
	primary := strategy.SelectPrimary(nodes)

	assert.Equal(t, "light", primary.NodeID)
}

// ---------------------------------------------------------------------------
// Tests: helper functions
// ---------------------------------------------------------------------------

func TestAppendUnique(t *testing.T) {
	vals := []string{"a", "b"}
	vals = appendUnique(vals, "c")
	assert.Equal(t, []string{"a", "b", "c"}, vals)

	vals = appendUnique(vals, "b")
	assert.Equal(t, []string{"a", "b", "c"}, vals, "should not duplicate")
}

func TestRemoveJob(t *testing.T) {
	vals := []string{"a", "b", "c"}
	vals = removeJob(vals, "b")
	assert.Equal(t, []string{"a", "c"}, vals)

	vals = removeJob(vals, "missing")
	assert.Equal(t, []string{"a", "c"}, vals, "removing non-existent should be no-op")
}
