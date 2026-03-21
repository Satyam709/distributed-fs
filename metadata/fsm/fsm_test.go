package fsm

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/stretchr/testify/assert"
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
	m.NodeRegistry[id] = &NodeEntry{
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
	m.FileIndex[fileID] = &FileRecord{
		FileID:    fileID,
		Filename:  name,
		FileSize:  100,
		ChunkIDs:  chunkIDs,
		Status:    FileStatusCreating,
		CreatedAt: time.Now(),
	}
	for i, cid := range chunkIDs {
		m.ChunkRegistry[cid] = &ChunkRecord{
			ChunkID:    cid,
			FileID:     fileID,
			ChunkIndex: i,
			Status:     ChunkStatusRequestAllocation,
		}
	}
}

// seedRepairJob inserts a repair job directly into the FSM.
func seedRepairJob(t *testing.T, m *MetadataFSM, jobID string) {
	t.Helper()
	now := time.Now()
	m.RepairJobRegistry[jobID] = &RepairJob{
		JobID:     jobID,
		Status:    RepairStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// Apply – top-level dispatch

func TestApply_InvalidJSON(t *testing.T) {
	m := newTestFSM(t)
	result := m.Apply(&raft.Log{Data: []byte("not-json")})
	require.Error(t, result.(error))
}

func TestApply_UnknownCommand(t *testing.T) {
	m := newTestFSM(t)
	log := makeRaftLog(t, MetadataCmdType(9999), struct{}{})
	result := m.Apply(log)
	require.Error(t, result.(error), "unknown command type must return an error")
}

func TestApply_RegisterNode_InvalidPayload(t *testing.T) {
	m := newTestFSM(t)
	// Build a MetadataCommand whose Payload is not valid JSON for CommandRegisterNode.
	cmd := MetadataCommand{Type: CmdRegisterNode, Payload: []byte("{")}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)

	// Apply returns the inner unmarshal error for malformed payloads.
	result := m.Apply(&raft.Log{Data: data})
	require.Error(t, result.(error), "malformed inner payload must surface as an error")
}

// Node Commands

func TestHandleCmdRegisterNode(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandRegisterNode
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:  "register new node",
			setup: func(m *MetadataFSM) {},
			req: CommandRegisterNode{
				NodeID:     "node-1",
				Address:    "10.0.0.1:9000",
				FreeSpace:  2048,
				ChunkCount: 5,
				CreatedAt:  now,
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				n, err := m.getNodeEntryForID("node-1")
				require.NoError(t, err)
				assert.Equal(t, "10.0.0.1:9000", n.Address)
				assert.Equal(t, NodeStatusAlive, n.Status)
				assert.Equal(t, uint64(2048), n.FreeSpace)
				assert.Equal(t, uint64(5), n.ChunkCount)
				assert.Equal(t, now, n.RegisteredAt)
				assert.Equal(t, now, n.UpdatedAt)
			},
		},
		{
			name: "re-register existing node updates fields",
			setup: func(m *MetadataFSM) {
				seedNode(t, m, "node-1", "old-addr:9000")
			},
			req: CommandRegisterNode{
				NodeID:     "node-1",
				Address:    "new-addr:9001",
				FreeSpace:  4096,
				ChunkCount: 10,
				CreatedAt:  now.Add(time.Minute),
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				n, err := m.getNodeEntryForID("node-1")
				require.NoError(t, err)
				assert.Equal(t, "new-addr:9001", n.Address)
				assert.Equal(t, NodeStatusAlive, n.Status)
				assert.Equal(t, uint64(4096), n.FreeSpace)
				assert.Equal(t, uint64(10), n.ChunkCount)
				assert.Equal(t, now.Add(time.Minute), n.UpdatedAt)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdRegisterNode(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdDeregisterNode(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandDeregisterNode
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:    "deregister non-existent node",
			setup:   func(m *MetadataFSM) {},
			req:     CommandDeregisterNode{NodeID: "ghost", UpdatedAt: baseTime},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				_, err := m.getNodeEntryForID("ghost")
				assert.ErrorIs(t, err, ErrNodeNotFound)
			},
		},
		{
			name: "deregister existing node sets status draining and advances timestamp",
			setup: func(m *MetadataFSM) {
				seedNode(t, m, "node-1", "addr:9000")
				m.NodeRegistry["node-1"].UpdatedAt = baseTime
			},
			req:     CommandDeregisterNode{NodeID: "node-1", UpdatedAt: baseTime.Add(time.Hour)},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				n, err := m.getNodeEntryForID("node-1")
				require.NoError(t, err)
				assert.Equal(t, NodeStatusDraining, n.Status)
				assert.Equal(t, baseTime.Add(time.Hour), n.UpdatedAt)
			},
		},
		{
			name: "deregister with older timestamp does not regress UpdatedAt",
			setup: func(m *MetadataFSM) {
				seedNode(t, m, "node-1", "addr:9000")
				m.NodeRegistry["node-1"].UpdatedAt = baseTime.Add(2 * time.Hour)
			},
			req:     CommandDeregisterNode{NodeID: "node-1", UpdatedAt: baseTime},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				n, err := m.getNodeEntryForID("node-1")
				require.NoError(t, err)
				assert.Equal(t, NodeStatusDraining, n.Status)
				assert.Equal(t, baseTime.Add(2*time.Hour), n.UpdatedAt,
					"UpdatedAt must not go backwards")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdDeregisterNode(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdMarkNodeDead(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandMarkNodeDead
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:    "non-existent node",
			setup:   func(m *MetadataFSM) {},
			req:     CommandMarkNodeDead{NodeID: "ghost", UpdatedAt: baseTime},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				_, err := m.getNodeEntryForID("ghost")
				assert.ErrorIs(t, err, ErrNodeNotFound)
			},
		},
		{
			name: "marks alive node as dead",
			setup: func(m *MetadataFSM) {
				seedNode(t, m, "node-2", "addr:8000")
				m.NodeRegistry["node-2"].UpdatedAt = baseTime
			},
			req:     CommandMarkNodeDead{NodeID: "node-2", UpdatedAt: baseTime.Add(5 * time.Minute)},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				n, _ := m.getNodeEntryForID("node-2")
				assert.Equal(t, NodeStatusDead, n.Status)
				assert.Equal(t, baseTime.Add(5*time.Minute), n.UpdatedAt)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdMarkNodeDead(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdMarkNodeAlive(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandMarkNodeAlive
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:    "non-existent node",
			setup:   func(m *MetadataFSM) {},
			req:     CommandMarkNodeAlive{NodeID: "ghost", UpdatedAt: baseTime},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "revive dead node",
			setup: func(m *MetadataFSM) {
				seedNode(t, m, "node-3", "addr:7000")
				m.NodeRegistry["node-3"].Status = NodeStatusDead
				m.NodeRegistry["node-3"].UpdatedAt = baseTime
			},
			req:     CommandMarkNodeAlive{NodeID: "node-3", UpdatedAt: baseTime.Add(10 * time.Minute)},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				n, _ := m.getNodeEntryForID("node-3")
				assert.Equal(t, NodeStatusAlive, n.Status)
				assert.Equal(t, baseTime.Add(10*time.Minute), n.UpdatedAt)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdMarkNodeAlive(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdUpdateNodeSpace(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandUpdateNodeSpace
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:    "non-existent node",
			setup:   func(m *MetadataFSM) {},
			req:     CommandUpdateNodeSpace{NodeID: "ghost", FreeSpace: 1, ChunkCount: 1, UpdatedAt: baseTime},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "update space and chunk count",
			setup: func(m *MetadataFSM) {
				seedNode(t, m, "node-4", "addr:6000")
				m.NodeRegistry["node-4"].UpdatedAt = baseTime
			},
			req: CommandUpdateNodeSpace{
				NodeID: "node-4", FreeSpace: 512, ChunkCount: 42,
				UpdatedAt: baseTime.Add(time.Minute),
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				n, _ := m.getNodeEntryForID("node-4")
				assert.Equal(t, uint64(512), n.FreeSpace)
				assert.Equal(t, uint64(42), n.ChunkCount)
				assert.Equal(t, baseTime.Add(time.Minute), n.UpdatedAt)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdUpdateNodeSpace(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

// File Commands

func TestHandleCmdCreateFile(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandCreateFile
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:  "create new file with chunks",
			setup: func(m *MetadataFSM) {},
			req: CommandCreateFile{
				FileID:    "file-1",
				FileName:  "photos.tar",
				ChunkIDs:  []string{"ck-1", "ck-2"},
				FileSize:  2048,
				Checksum:  []byte{0xAB, 0xCD},
				CreatedAt: now,
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, err := m.getFileEntryForID("file-1")
				require.NoError(t, err)
				assert.Equal(t, "photos.tar", f.Filename)
				assert.Equal(t, FileStatusCreating, f.Status)
				assert.Equal(t, uint64(2048), f.FileSize)
				assert.Len(t, f.ChunkIDs, 2)

				for i, cid := range []string{"ck-1", "ck-2"} {
					c, err := m.getChunkEntryForID(cid)
					require.NoError(t, err)
					assert.Equal(t, "file-1", c.FileID)
					assert.Equal(t, i, c.ChunkIndex)
					assert.Equal(t, ChunkStatusRequestAllocation, c.Status)
				}
			},
		},
		{
			name: "reject duplicate file ID",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "old.txt", []string{"old-ck-1"})
			},
			req: CommandCreateFile{
				FileID:   "file-1",
				FileName: "new.txt",
				ChunkIDs: []string{"ck-new"},
				FileSize: 100,
			},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				// original file must be untouched
				f, _ := m.getFileEntryForID("file-1")
				assert.Equal(t, "old.txt", f.Filename)
			},
		},
		{
			name: "reject when chunk ID collides with existing registry",
			setup: func(m *MetadataFSM) {
				// pre-populate a chunk that conflicts
				m.ChunkRegistry["ck-dup"] = &ChunkRecord{ChunkID: "ck-dup"}
			},
			req: CommandCreateFile{
				FileID:   "file-2",
				FileName: "test.bin",
				ChunkIDs: []string{"ck-dup"},
				FileSize: 50,
			},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				_, err := m.getFileEntryForID("file-2")
				assert.ErrorIs(t, err, ErrFileNotFound, "file must not be created on chunk collision")
			},
		},
		{
			name:  "create file with zero chunks",
			setup: func(m *MetadataFSM) {},
			req: CommandCreateFile{
				FileID:    "file-empty",
				FileName:  "empty.txt",
				ChunkIDs:  []string{},
				FileSize:  0,
				CreatedAt: now,
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, err := m.getFileEntryForID("file-empty")
				require.NoError(t, err)
				assert.Len(t, f.ChunkIDs, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdCreateFile(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdCommitFile(t *testing.T) {
	checksum := []byte{0x01, 0x02}

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandCommitFile
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:    "file does not exist",
			setup:   func(m *MetadataFSM) {},
			req:     CommandCommitFile{FileID: "ghost", FileSize: 100, Checksum: checksum},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "filesize mismatch",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{})
				m.FileIndex["file-1"].CheckSum = checksum
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 999, Checksum: checksum},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.getFileEntryForID("file-1")
				assert.Equal(t, FileStatusCreating, f.Status, "status must not change on error")
			},
		},
		{
			name: "checksum mismatch",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{})
				m.FileIndex["file-1"].CheckSum = checksum
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 100, Checksum: []byte{0xFF}},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.getFileEntryForID("file-1")
				assert.Equal(t, FileStatusCreating, f.Status)
			},
		},
		{
			name: "chunk not in complete status",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{"ck-1"})
				m.FileIndex["file-1"].CheckSum = checksum
				// chunk is still in RequestAllocation status
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 100, Checksum: checksum},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.getFileEntryForID("file-1")
				assert.Equal(t, FileStatusCreating, f.Status)
			},
		},
		{
			name: "successful commit with all chunks complete",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "f.bin", []string{"ck-1", "ck-2"})
				m.FileIndex["file-1"].CheckSum = checksum
				m.ChunkRegistry["ck-1"].Status = ChunkStatusComplete
				m.ChunkRegistry["ck-2"].Status = ChunkStatusComplete
			},
			req:     CommandCommitFile{FileID: "file-1", FileSize: 100, Checksum: checksum},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.getFileEntryForID("file-1")
				assert.Equal(t, FileStatusComplete, f.Status)
			},
		},
		{
			name: "commit file with zero chunks",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-e", "empty.bin", []string{})
				m.FileIndex["file-e"].CheckSum = checksum
				m.FileIndex["file-e"].FileSize = 0
			},
			req:     CommandCommitFile{FileID: "file-e", FileSize: 0, Checksum: checksum},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.getFileEntryForID("file-e")
				assert.Equal(t, FileStatusComplete, f.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdCommitFile(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdDeleteFile(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandDeleteFile
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:    "delete non-existent file",
			setup:   func(m *MetadataFSM) {},
			req:     CommandDeleteFile{FileID: "ghost"},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "soft-delete existing file",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "file-1", "doomed.txt", []string{"ck-1"})
			},
			req:     CommandDeleteFile{FileID: "file-1"},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				f, _ := m.getFileEntryForID("file-1")
				assert.Equal(t, FileStatusDeleted, f.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdDeleteFile(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

// Chunk Commands

func TestHandleCmdCommitChunk(t *testing.T) {
	checksum := []byte{0xDE, 0xAD}

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandCommitChunk
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:    "chunk does not exist",
			setup:   func(m *MetadataFSM) {},
			req:     CommandCommitChunk{ChunkID: "ghost", NodeIDs: []string{"n1"}, Checksum: checksum},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "successful first commit",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "a.bin", []string{"ck-1"})
			},
			req:     CommandCommitChunk{ChunkID: "ck-1", NodeIDs: []string{"n1", "n2"}, Checksum: checksum},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				c, _ := m.getChunkEntryForID("ck-1")
				assert.Equal(t, ChunkStatusComplete, c.Status)
				assert.Equal(t, checksum, c.Checksum)
				assert.Equal(t, []string{"n1", "n2"}, c.Replicas)
			},
		},
		{
			name: "reject re-commit when chunk already complete with checksum",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "a.bin", []string{"ck-1"})
				m.ChunkRegistry["ck-1"].Status = ChunkStatusComplete
				m.ChunkRegistry["ck-1"].Checksum = checksum
			},
			req:     CommandCommitChunk{ChunkID: "ck-1", NodeIDs: []string{"n3"}, Checksum: checksum},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				// replicas must remain unchanged
				c, _ := m.getChunkEntryForID("ck-1")
				assert.NotContains(t, c.Replicas, "n3")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdCommitChunk(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdEvictChunkFromNode(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandEvictChunkFromNode
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:  "chunk does not exist",
			setup: func(m *MetadataFSM) {},
			req: CommandEvictChunkFromNode{
				ChunkID: "ghost", NodeID: "n1", EvictedAt: now, Reason: "test",
			},
			wantErr:    true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "node not in replicas",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "b.bin", []string{"ck-1"})
				m.ChunkRegistry["ck-1"].Replicas = []string{"n1", "n2"}
			},
			req: CommandEvictChunkFromNode{
				ChunkID: "ck-1", NodeID: "n3", EvictedAt: now, Reason: "test",
			},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				c, _ := m.getChunkEntryForID("ck-1")
				assert.Len(t, c.Replicas, 2, "replicas must be unchanged")
			},
		},
		{
			name: "evict first replica",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "b.bin", []string{"ck-1"})
				m.ChunkRegistry["ck-1"].Replicas = []string{"n1", "n2", "n3"}
			},
			req: CommandEvictChunkFromNode{
				ChunkID: "ck-1", NodeID: "n1", EvictedAt: now, Reason: "disk-fail",
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				c, _ := m.getChunkEntryForID("ck-1")
				assert.Equal(t, []string{"n2", "n3"}, c.Replicas)
				assert.NotContains(t, c.Replicas, "n1")
			},
		},
		{
			name: "evict last replica",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "b.bin", []string{"ck-1"})
				m.ChunkRegistry["ck-1"].Replicas = []string{"n1", "n2", "n3"}
			},
			req: CommandEvictChunkFromNode{
				ChunkID: "ck-1", NodeID: "n3", EvictedAt: now, Reason: "decom",
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				c, _ := m.getChunkEntryForID("ck-1")
				assert.Equal(t, []string{"n1", "n2"}, c.Replicas)
			},
		},
		{
			name: "evict sole replica leaves empty slice",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "b.bin", []string{"ck-1"})
				m.ChunkRegistry["ck-1"].Replicas = []string{"n1"}
			},
			req: CommandEvictChunkFromNode{
				ChunkID: "ck-1", NodeID: "n1", EvictedAt: now, Reason: "test",
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				c, _ := m.getChunkEntryForID("ck-1")
				assert.Empty(t, c.Replicas)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdEvictChunkFromNode(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdMarkChunkLost(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandMarkChunkLost
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:       "chunk does not exist",
			setup:      func(m *MetadataFSM) {},
			req:        CommandMarkChunkLost{ChunkID: "ghost"},
			wantErr:    true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "mark allocated chunk as lost",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "c.bin", []string{"ck-1"})
			},
			req:     CommandMarkChunkLost{ChunkID: "ck-1"},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				c, _ := m.getChunkEntryForID("ck-1")
				assert.Equal(t, ChunkStatusLost, c.Status)
			},
		},
		{
			name: "mark complete chunk as lost",
			setup: func(m *MetadataFSM) {
				seedFile(t, m, "f1", "c.bin", []string{"ck-1"})
				m.ChunkRegistry["ck-1"].Status = ChunkStatusComplete
			},
			req:     CommandMarkChunkLost{ChunkID: "ck-1"},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				c, _ := m.getChunkEntryForID("ck-1")
				assert.Equal(t, ChunkStatusLost, c.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdMarkChunkLost(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

// Repair Job Commands

func TestHandleCmdCreateRepairJob(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandCreateRepairJob
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:  "create new job",
			setup: func(m *MetadataFSM) {},
			req: CommandCreateRepairJob{
				JobID:     "job-1",
				CreatedAt: now,
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				j, err := m.getJobEntryForID("job-1")
				require.NoError(t, err)
				assert.Equal(t, RepairStatusPending, j.Status)
				assert.Equal(t, uint64(0), j.Attempts)
				assert.Equal(t, now, j.CreatedAt)
				assert.Equal(t, now, j.UpdatedAt)
			},
		},
		{
			name: "reject duplicate job ID",
			setup: func(m *MetadataFSM) {
				seedRepairJob(t, m, "job-1")
			},
			req:     CommandCreateRepairJob{JobID: "job-1", CreatedAt: now},
			wantErr: true,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				j, _ := m.getJobEntryForID("job-1")
				assert.Equal(t, RepairStatusPending, j.Status, "existing job must be untouched")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdCreateRepairJob(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

func TestHandleCmdUpdateRepairJob(t *testing.T) {
	baseTime := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		setup      func(*MetadataFSM)
		req        CommandUpdateRepairJob
		wantErr    bool
		assertPost func(*testing.T, *MetadataFSM)
	}{
		{
			name:  "job does not exist",
			setup: func(m *MetadataFSM) {},
			req: CommandUpdateRepairJob{
				JobID: "ghost", Status: RepairStatusDone,
				UpdatedAt: baseTime, Attempts: 1,
			},
			wantErr:    true,
			assertPost: func(t *testing.T, m *MetadataFSM) {},
		},
		{
			name: "update pending job to in-progress",
			setup: func(m *MetadataFSM) {
				seedRepairJob(t, m, "job-1")
			},
			req: CommandUpdateRepairJob{
				JobID: "job-1", Status: RepairStatusInProgress,
				UpdatedAt: baseTime, Attempts: 1,
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				j, _ := m.getJobEntryForID("job-1")
				assert.Equal(t, RepairStatusInProgress, j.Status)
				assert.Equal(t, uint64(1), j.Attempts)
				assert.Equal(t, baseTime, j.UpdatedAt)
			},
		},
		{
			name: "update job to failed with error message",
			setup: func(m *MetadataFSM) {
				seedRepairJob(t, m, "job-2")
			},
			req: CommandUpdateRepairJob{
				JobID: "job-2", Status: RepairStatusFailed,
				UpdatedAt: baseTime, Attempts: 3,
				Error: "source node unreachable",
			},
			wantErr: false,
			assertPost: func(t *testing.T, m *MetadataFSM) {
				j, _ := m.getJobEntryForID("job-2")
				assert.Equal(t, RepairStatusFailed, j.Status)
				assert.Equal(t, uint64(3), j.Attempts)
				assert.Equal(t, "source node unreachable", j.Error)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestFSM(t)
			tc.setup(m)

			err := m.handleCmdUpdateRepairJob(tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			tc.assertPost(t, m)
		})
	}
}

// Helpers (upsert + getters)

func TestUpsert(t *testing.T) {
	tests := []struct {
		name     string
		registry map[string]int
		mu       sync.Locker
		key      string
		value    int
		wantErr  bool
	}{
		{
			name:     "insert into valid registry",
			registry: map[string]int{},
			mu:       &sync.Mutex{},
			key:      "k1",
			value:    42,
			wantErr:  false,
		},
		{
			name:     "overwrite existing key",
			registry: map[string]int{"k1": 1},
			mu:       &sync.Mutex{},
			key:      "k1",
			value:    99,
			wantErr:  false,
		},
		{
			name:     "nil registry returns error",
			registry: nil,
			mu:       &sync.Mutex{},
			key:      "k1",
			value:    1,
			wantErr:  true,
		},
		{
			name:     "nil mutex is allowed",
			registry: map[string]int{},
			mu:       nil,
			key:      "k1",
			value:    7,
			wantErr:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := upsert(tc.mu, tc.registry, tc.key, tc.value)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.value, tc.registry[tc.key])
			}
		})
	}
}

func TestGetters_NotFound(t *testing.T) {
	m := newTestFSM(t)

	tests := []struct {
		name    string
		run     func() error
		wantErr error
	}{
		{
			name:    "getNodeEntryForID",
			run:     func() error { _, err := m.getNodeEntryForID("x"); return err },
			wantErr: ErrNodeNotFound,
		},
		{
			name:    "getFileEntryForID",
			run:     func() error { _, err := m.getFileEntryForID("x"); return err },
			wantErr: ErrFileNotFound,
		},
		{
			name:    "getChunkEntryForID",
			run:     func() error { _, err := m.getChunkEntryForID("x"); return err },
			wantErr: ErrChunkNotFound,
		},
		{
			name:    "getJobEntryForID",
			run:     func() error { _, err := m.getJobEntryForID("x"); return err },
			wantErr: ErrJobNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			assert.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// Apply – end-to-end via raft.Log (integration-style)

func TestApply_RegisterNode_EndToEnd(t *testing.T) {
	m := newTestFSM(t)
	now := time.Now()

	req := CommandRegisterNode{
		NodeID:     "e2e-node",
		Address:    "192.168.1.1:5000",
		FreeSpace:  8192,
		ChunkCount: 0,
		CreatedAt:  now,
	}
	log := makeRaftLog(t, CmdRegisterNode, req)
	result := m.Apply(log)
	assert.Nil(t, result, "successful Apply should return nil error")

	n, err := m.getNodeEntryForID("e2e-node")
	require.NoError(t, err)
	assert.Equal(t, "192.168.1.1:5000", n.Address)
	assert.Equal(t, NodeStatusAlive, n.Status)
}
