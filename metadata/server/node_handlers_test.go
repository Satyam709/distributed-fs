package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRepairInstructions_IncludesSourceAndTargetAddresses(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())
	now := time.Now()

	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-src", Address: "10.0.0.1:4000", FreeSpace: 1024, CreatedAt: now,
	})
	applyFSMCommand(t, m, fsm.CmdRegisterNode, fsm.CommandRegisterNode{
		NodeID: "node-dst", Address: "10.0.0.2:4000", FreeSpace: 1024, CreatedAt: now,
	})

	applyFSMCommand(t, m, fsm.CmdCreateRepairJob, fsm.CommandCreateRepairJob{
		JobID:        "job-1",
		CreatedAt:    now,
		ChunkID:      "chunk-1",
		DeleteSource: true,
		SourceNode:   "node-src",
		TargetNode:   "node-dst",
	})

	ins := buildRepairInstructions(m, []string{"job-1"}, logging.NewCLogger())
	require.Len(t, ins, 1)
	assert.Equal(t, "job-1", ins[0].GetJobId())
	assert.Equal(t, "chunk-1", ins[0].GetChunkId())
	assert.Equal(t, "node-src", ins[0].GetSourceNodeId())
	assert.Equal(t, "10.0.0.1:4000", ins[0].GetSourceAddr())
	assert.Equal(t, "node-dst", ins[0].GetTargetNodeId())
	assert.Equal(t, "10.0.0.2:4000", ins[0].GetTargetAddr())
	assert.True(t, ins[0].GetDeleteSource())
}

func TestBuildRepairInstructions_SkipsMissingJobs(t *testing.T) {
	m := fsm.NewEmptyMetadataFsm(logging.NewCLogger())

	ins := buildRepairInstructions(m, []string{"missing-job"}, logging.NewCLogger())
	assert.Empty(t, ins)
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
