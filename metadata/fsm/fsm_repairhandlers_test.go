package fsm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				j, err := m.GetRepairJob("job-1")
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
				j, _ := m.GetRepairJob("job-1")
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
				j, _ := m.GetRepairJob("job-1")
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
				j, _ := m.GetRepairJob("job-2")
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
