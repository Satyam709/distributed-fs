package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type placementSpy struct {
	reverseCount int
}

func (p *placementSpy) SelectNodes(_ string, _ int, _ ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	return nil, nil
}

func (p *placementSpy) SelectNodeReverse(_ string, count int, _ ...fsm.NodeEntry) ([]fsm.NodeEntry, error) {
	p.reverseCount = count
	return nil, nil
}

func (p *placementSpy) SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry {
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

func TestScheduleRepairJobs_OverReplicatedUsesPositiveReverseCount(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	placement := &placementSpy{}
	rs := NewRepairScheduler(nil, m, placement, 3)

	rs.scheduleRepairJobs("chunk-1", nil, -2)

	assert.Equal(t, 2, placement.reverseCount)
}

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

	rs := NewRepairScheduler(nil, m, &placementSpy{}, 3)
	rs.RecoverStuckJobs(context.Background())

	requeued := map[string]bool{}
	for len(rs.jobs) > 0 {
		requeued[<-rs.jobs] = true
	}

	assert.Len(t, requeued, 2)
	assert.True(t, requeued["job-1"])
	assert.True(t, requeued["job-2"])
}
