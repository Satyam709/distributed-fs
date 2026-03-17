package store

import (
	"fmt"

	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// BoltStore implements both raft.LogStore and raft.StableStore
// using a single BoltDB file on disk.
type BoltStore struct {
	store *raftboltdb.BoltStore
}

func NewBoltStore(path string) (*raftboltdb.BoltStore, error) {
	db, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		return nil, fmt.Errorf("open bolt store at %s: %w", path, err)
	}
	return db, nil
}
