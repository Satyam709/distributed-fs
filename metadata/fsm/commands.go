package fsm

import "github.com/hashicorp/raft"

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
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	FreeSpace string `json:"freespace"`
}

type CommandDeregisterNode struct {
	NodeID string `json:"node_id"`
}

type CommandMarkNodeDead struct {
	NodeID string `json:"node_id"`
}
type CommandMarkNodeAlive struct {
	NodeID string `json:"node_id"`
}

type CommandUpdateNodeSpace struct {
	NodeID string `json:"node_id"`
}

// FILE commands
type CommandCreateFile struct {
	FileID   string   `json:"file_id"`
	FileName string   `json:"filename"`
	ChunkIDs []string `json:"chunk_ids"`
	FileSize uint64   `json:"filesize"`
	Checksum []byte   `json:"checksum"`
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
	ChunkID string `json:"chunk_id"`
}

type CommandMarkChunkLost struct {
	ChunkID string `json:"chunk_id"`
}

// JOBS

type ComamndCreateRepairJob struct {
}

type ComamndUpdateRepairJob struct {
	JobID string `json:"job_id"`
}

// Propose proposes the cmd to the given raft instance
func Propose(raft *raft.Raft, cmd MetadataCommand) {

}
