package fsm

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/stretchr/testify/require"
)

// helpers

func newTestFSM(t *testing.T) *MetadataFSM {
	t.Helper()
	return NewEmptyMetadataFsm(logging.NewCLogger())
}

// makeRaftLog serialises a MetadataCommand into a raft.Log.
func makeRaftLog(t *testing.T, cmdType MetadataCmdType, payload any) *raft.Log {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	cmd := MetadataCommand{Type: cmdType, Payload: raw}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)

	return &raft.Log{Data: data}
}

// seedNode inserts a node directly so that handlers that depend on existing
// state have something to operate on.
func seedNode(t *testing.T, m *MetadataFSM, id, addr string) {
	t.Helper()
	now := time.Now()
	m.nodeRegistry[id] = &NodeEntry{
		NodeID:       id,
		Address:      addr,
		Status:       NodeStatusAlive,
		FreeSpace:    1000,
		ChunkCount:   0,
		RegisteredAt: now,
		UpdatedAt:    now,
	}
}

// seedFile inserts a file record and its associated chunks.
func seedFile(t *testing.T, m *MetadataFSM, fileID, name string, chunkIDs []string) {
	t.Helper()
	m.fileIndex[fileID] = &FileRecord{
		FileID:    fileID,
		Filename:  name,
		FileSize:  100,
		ChunkIDs:  chunkIDs,
		Status:    FileStatusCreating,
		CreatedAt: time.Now(),
	}
	for i, cid := range chunkIDs {
		m.chunkRegistry[cid] = &ChunkRecord{
			ChunkID:    cid,
			FileID:     fileID,
			ChunkIndex: i,
			Status:     ChunkStatusAllocated,
		}
	}
}

// seedRepairJob inserts a repair job directly into the FSM.
func seedRepairJob(t *testing.T, m *MetadataFSM, jobID string) {
	t.Helper()
	now := time.Now()
	m.repairJobRegistry[jobID] = &RepairJob{
		JobID:     jobID,
		Status:    RepairStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
