package fsm

import (
	"encoding/json"
	"time"

	"github.com/hashicorp/raft"
)

type MetadataCmdType int

// TODO: Add doc
const (
	// Node Commands
	CmdRegisterNode MetadataCmdType = iota
	CmdDeregisterNode
	CmdMarkNodeDead
	CmdMarkNodeAlive
	CmdUpdateNodeSpace

	// File Commands
	CmdCreateFile
	CmdCommitFile
	CmdDeleteFile
	CmdCommitChunk
	CmdEvictChunkFromNode
	CmdMarkChunkLost

	// Jobs Command
	CmdCreateRepairJob
	CmdUpdateRepairJob
)

// MetadataCommand is a generic cmd, executable on fsm
type MetadataCommand struct {
	Type    MetadataCmdType
	Payload []byte
}
type CommandRegisterNode struct {
	NodeID     string    `json:"node_id"`
	Address    string    `json:"address"`
	FreeSpace  uint64    `json:"freespace"`
	ChunkCount uint64    `json:"chunkcount"`
	CreatedAt  time.Time `json:"created_at"`
}

type CommandDeregisterNode struct {
	NodeID    string    `json:"node_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CommandMarkNodeDead struct {
	NodeID    string    `json:"node_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CommandMarkNodeAlive struct {
	NodeID    string    `json:"node_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CommandUpdateNodeSpace struct {
	NodeID     string    `json:"node_id"`
	FreeSpace  uint64    `json:"freespace"`
	ChunkCount uint64    `json:"chunkcount"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// FILE commands
type CommandCreateFile struct {
	FileID    string    `json:"file_id"`
	FileName  string    `json:"filename"`
	ChunkIDs  []string  `json:"chunk_ids"`
	FileSize  uint64    `json:"filesize"`
	Checksum  []byte    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
}
type CommandCommitFile struct {
	FileID   string `json:"file_id"`
	FileSize uint64 `json:"filesize"`
	Checksum []byte `json:"checksum"`
}

type CommandDeleteFile struct {
	FileID string `json:"file_id"`
}

type CommandCommitChunk struct {
	ChunkID  string   `json:"chunk_id"`
	NodeIDs  []string `json:"node_ids"`
	Checksum []byte   `json:"checksum"`
}

type CommandEvictChunkFromNode struct {
	ChunkID   string    `json:"chunk_id"`
	NodeID    string    `json:"node_id"`
	EvictedAt time.Time `json:"evicted_at"`
	Reason    string    `json:"reason"`
}

type CommandMarkChunkLost struct {
	ChunkID string `json:"chunk_id"`
}

// JOBS

type CommandCreateRepairJob struct {
	JobID     string    `json:"job_id"`
	CreatedAt time.Time `json:"created_at"`
}

type CommandUpdateRepairJob struct {
	JobID     string       `json:"job_id"`
	Status    RepairStatus `json:"status"`
	UpdatedAt time.Time    `json:"updated_at"`
	Attempts  uint64       `json:"attempts"`
	Error     string       `json:"error"`
}

// Propose proposes the cmd to the given raft instance
func Propose(raft *raft.Raft, cmd MetadataCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := raft.Apply(data, 5*time.Second)
	return f.Error()
}
