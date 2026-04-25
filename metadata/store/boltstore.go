package store

import (
	"fmt"

	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func NewBoltStore(path string) (*raftboltdb.BoltStore, error) {
	db, err := raftboltdb.NewBoltStore(path)
	if err != nil {
		return nil, fmt.Errorf("open bolt store at %s: %w", path, err)
	}
	return db, nil
}
