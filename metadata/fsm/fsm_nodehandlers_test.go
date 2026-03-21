package fsm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				n, err := m.GetNode("node-1")
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
				n, err := m.GetNode("node-1")
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
				_, err := m.GetNode("ghost")
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
				n, err := m.GetNode("node-1")
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
				n, err := m.GetNode("node-1")
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
				_, err := m.GetNode("ghost")
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
				n, _ := m.GetNode("node-2")
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
			name:       "non-existent node",
			setup:      func(m *MetadataFSM) {},
			req:        CommandMarkNodeAlive{NodeID: "ghost", UpdatedAt: baseTime},
			wantErr:    true,
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
				n, _ := m.GetNode("node-3")
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
			name:       "non-existent node",
			setup:      func(m *MetadataFSM) {},
			req:        CommandUpdateNodeSpace{NodeID: "ghost", FreeSpace: 1, ChunkCount: 1, UpdatedAt: baseTime},
			wantErr:    true,
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
				n, _ := m.GetNode("node-4")
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
