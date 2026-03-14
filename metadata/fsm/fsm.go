package fsm

import (
	"io"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/metadata"
)

type MetadataFSM struct {
	FileIndex         map[string]metadata.FileRecord
	ChunkRegistry     map[string]metadata.ChunkRecord
	NodeRegistry      map[string]metadata.NodeEntry
	RepairJobRegistry map[string]metadata.RepairJob
}

func (mfsm *MetadataFSM) Apply(rlog *raft.Log) interface{} {
	
	return nil
}

func (mfsm *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (mfsm *MetadataFSM) Restore(snapshot io.ReadCloser) error {
	return nil
}
