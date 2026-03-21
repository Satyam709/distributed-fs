package fsm

import (
	"encoding/json"
	"time"

	"github.com/hashicorp/raft"
)

// MetadataCmdType identifies the kind of mutation being applied to the FSM.
// Every value maps 1:1 to a concrete Command* struct and a handler in
// MetadataFSM.Apply().
type MetadataCmdType int

const (
	// Node commands

	// CmdRegisterNode — triggered by storage-node startup.
	// Adds or re-activates a NodeEntry in the NodeRegistry.
	CmdRegisterNode MetadataCmdType = iota

	// CmdDeregisterNode — triggered by graceful storage-node shutdown.
	// Marks the node as "draining".
	CmdDeregisterNode

	// CmdMarkNodeDead — triggered by NodeWatcher heartbeat timeout.
	// Marks the node as "dead" and removes it from placement eligibility.
	CmdMarkNodeDead

	// CmdMarkNodeAlive — triggered by node re-registration.
	// Transitions the node back to "alive".
	CmdMarkNodeAlive

	// CmdUpdateNodeSpace — triggered periodically from heartbeat data.
	// Updates FreeSpace and ChunkCount on the NodeEntry.
	CmdUpdateNodeSpace

	// File commands

	// CmdCreateFile — triggered by client upload start.
	// Adds a FileRecord (status: creating) and stub ChunkRecords.
	CmdCreateFile

	// CmdCommitFile — triggered by client after all chunks are done.
	// Marks the FileRecord as complete after validating every chunk.
	CmdCommitFile

	// CmdDeleteFile — triggered by client delete request.
	// Marks the FileRecord as deleted; chunk eviction happens async.
	CmdDeleteFile

	// CmdCommitChunk — triggered by storage node after replication.
	// Records the confirmed replica list and checksum, marks complete.
	CmdCommitChunk

	// CmdEvictChunkFromNode — triggered by reconciliation or node death.
	// Removes a node from the chunk's replica list.
	CmdEvictChunkFromNode

	// CmdMarkChunkLost — triggered when repair is exhausted.
	// Marks the chunk as permanently lost (no live replicas).
	CmdMarkChunkLost

	// Repair-job commands

	// CmdCreateRepairJob — triggered by RepairScheduler.
	// Adds a new RepairJob to the RepairJobRegistry.
	CmdCreateRepairJob

	// CmdUpdateRepairJob — triggered by storage-node repair result.
	// Updates RepairJob status (done/failed), attempt count, and error.
	CmdUpdateRepairJob
)

// MetadataCommand is the generic envelope serialised into every Raft log
// entry. Type selects the handler; Payload carries the command-specific
// struct encoded as JSON.
type MetadataCommand struct {
	Type    MetadataCmdType `json:"type"`
	Payload []byte          `json:"payload"`
}

// CommandRegisterNode carries the data needed to register a new storage
// node or re-activate an existing one.
type CommandRegisterNode struct {
	NodeID     string    `json:"node_id"`
	Address    string    `json:"address"`
	FreeSpace  uint64    `json:"freespace"`
	ChunkCount uint64    `json:"chunkcount"`
	CreatedAt  time.Time `json:"created_at"`
}

// CommandDeregisterNode marks a node as draining (graceful shutdown).
type CommandDeregisterNode struct {
	NodeID    string    `json:"node_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CommandMarkNodeDead transitions a node to "dead" after heartbeat timeout.
type CommandMarkNodeDead struct {
	NodeID    string    `json:"node_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CommandMarkNodeAlive transitions a node back to "alive" after re-registration.
type CommandMarkNodeAlive struct {
	NodeID    string    `json:"node_id"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CommandUpdateNodeSpace updates free-space and chunk-count metrics for
// a storage node. Proposed periodically from heartbeat data.
type CommandUpdateNodeSpace struct {
	NodeID     string    `json:"node_id"`
	FreeSpace  uint64    `json:"freespace"`
	ChunkCount uint64    `json:"chunkcount"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// File commands

// CommandCreateFile records a new file and its ordered chunk ID list.
type CommandCreateFile struct {
	FileID    string    `json:"file_id"`
	FileName  string    `json:"filename"`
	ChunkIDs  []string  `json:"chunk_ids"`
	FileSize  uint64    `json:"filesize"`
	Checksum  []byte    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
}

// CommandCommitFile transitions a file from "creating" to "complete"
// after validating size, checksum, and chunk statuses.
type CommandCommitFile struct {
	FileID   string `json:"file_id"`
	FileSize uint64 `json:"filesize"`
	Checksum []byte `json:"checksum"`
}

// CommandDeleteFile marks a file as deleted. Chunk eviction is async.
type CommandDeleteFile struct {
	FileID string `json:"file_id"`
}

// CommandCommitChunk finalises a chunk with the confirmed replica list
// and checksum after replication completes on the storage nodes.
type CommandCommitChunk struct {
	ChunkID  string   `json:"chunk_id"`
	NodeIDs  []string `json:"node_ids"` // storage nodes that confirmed holding the chunk
	Checksum []byte   `json:"checksum"`
}

// CommandEvictChunkFromNode removes a node from a chunk's replica list.
// Used during reconciliation, corruption reporting, or node death.
type CommandEvictChunkFromNode struct {
	ChunkID   string    `json:"chunk_id"`
	NodeID    string    `json:"node_id"`
	EvictedAt time.Time `json:"evicted_at"`
	Reason    string    `json:"reason"` // human-readable eviction reason for audit
}

// CommandMarkChunkLost marks a chunk as permanently lost when repair
// is exhausted and no live replicas remain.
type CommandMarkChunkLost struct {
	ChunkID string `json:"chunk_id"`
}

// Repair-job commands

// CommandCreateRepairJob creates a new repair job in pending state.
type CommandCreateRepairJob struct {
	JobID     string    `json:"job_id"`
	CreatedAt time.Time `json:"created_at"`
}

// CommandUpdateRepairJob mutates an existing repair job's status,
// attempt counter, and optional error message.
type CommandUpdateRepairJob struct {
	JobID     string       `json:"job_id"`
	Status    RepairStatus `json:"status"`
	UpdatedAt time.Time    `json:"updated_at"`
	Attempts  uint64       `json:"attempts"`
	Error     string       `json:"error"` // empty on success
}

// Propose serialises the given MetadataCommand and submits it to the
// Raft cluster via raft.Apply. It blocks until the entry is committed
// by a majority of nodes (or the 5-second timeout expires).
//
// This is the single chokepoint for all state-mutating operations.
// Only the Raft leader may call Propose; followers must reject writes
// and redirect clients to the current leader.
func Propose(raft *raft.Raft, cmd MetadataCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := raft.Apply(data, 5*time.Second)
	return f.Error()
}
