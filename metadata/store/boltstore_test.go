package store

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestNewBoltStore_Success(t *testing.T) {
	dir := t.TempDir()

	dbPath := filepath.Join(dir, "raft.db")

	store, err := NewBoltStore(dbPath)

	require.NoError(t, err)
	require.NotNil(t, store)

	store.Close()
}

func TestNewBoltStore_InvalidPath(t *testing.T) {
	path := "/invalid/path/raft.db"

	store, err := NewBoltStore(path)

	require.Error(t, err)
	require.Nil(t, store)
}

func TestNewBoltStore_ErrorMessageContainsPath(t *testing.T) {
	path := "/invalid/path/raft.db"

	_, err := NewBoltStore(path)

	require.Error(t, err)
	assert.Contains(t, err.Error(), path)
}
