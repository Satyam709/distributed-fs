package fsm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			name:       "chunk does not exist",
			setup:      func(m *MetadataFSM) {},
			req:        CommandCommitChunk{ChunkID: "ghost", NodeIDs: []string{"n1"}, Checksum: checksum},
			wantErr:    true,
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
				c, _ := m.GetChunk("ck-1")
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
				c, _ := m.GetChunk("ck-1")
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
				c, _ := m.GetChunk("ck-1")
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
				c, _ := m.GetChunk("ck-1")
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
				c, _ := m.GetChunk("ck-1")
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
				c, _ := m.GetChunk("ck-1")
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
				c, _ := m.GetChunk("ck-1")
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
				c, _ := m.GetChunk("ck-1")
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
