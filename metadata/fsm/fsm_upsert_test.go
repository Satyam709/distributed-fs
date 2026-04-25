package fsm

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			run:     func() error { _, err := m.GetNode("x"); return err },
			wantErr: ErrNodeNotFound,
		},
		{
			name:    "getFileEntryForID",
			run:     func() error { _, err := m.GetFile("x"); return err },
			wantErr: ErrFileNotFound,
		},
		{
			name:    "getChunkEntryForID",
			run:     func() error { _, err := m.GetChunk("x"); return err },
			wantErr: ErrChunkNotFound,
		},
		{
			name:    "getJobEntryForID",
			run:     func() error { _, err := m.GetRepairJob("x"); return err },
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
