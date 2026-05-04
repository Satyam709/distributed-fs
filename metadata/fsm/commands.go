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

	// CmdAddChunkReplica — triggered by RepairScheduler after successful repair.
	// Adds a single node to a chunk's replica list.
	CmdAddChunkReplica

	// Metadata node commands

	// CmdRegisterMetadataNode — registers or updates metadata node raft→grpc mapping.
	CmdRegisterMetadataNode

	// CmdDeregisterMetadataNode — removes a metadata node from the address registry.
	CmdDeregisterMetadataNode
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
	ChunkSize uint64    `json:"chunk_size"`
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
// TargetNode=nil, is used to indicate that its a nil replication
// used with DeleteSource = true, in overreplicated chunks case
type CommandCreateRepairJob struct {
	JobID        string    `json:"job_id"`
	CreatedAt    time.Time `json:"created_at"`
	ChunkID      string    `json:"chunk_id"`
	DeleteSource bool      `json:"delete_source"`
	SourceNode   string    `json:"source_node"`
	TargetNode   string    `json:"target_node"`
}

// CommandUpdateRepairJob mutates an existing repair job's status,
// attempt counter, and optional error message.
type CommandUpdateRepairJob struct {
	JobID      string       `json:"job_id"`
	Status     RepairStatus `json:"status"`
	SourceNode string       `json:"source_node"`
	UpdatedAt  time.Time    `json:"updated_at"`
	Attempts   uint64       `json:"attempts"`
	Error      string       `json:"error"` // empty on success
}

// CommandAddChunkReplica adds a single node to a chunk's replica list.
// Used by the RepairScheduler after a successful repair operation.
type CommandAddChunkReplica struct {
	ChunkID string `json:"chunk_id"`
	NodeID  string `json:"node_id"`
}

// CommandRegisterMetadataNode stores or updates the raft→grpc address
// mapping for a metadata cluster member.
type CommandRegisterMetadataNode struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"`
	GrpcAddr string `json:"grpc_addr"`
}

// CommandDeregisterMetadataNode removes a metadata node from the address
// registry, typically during graceful shutdown.
type CommandDeregisterMetadataNode struct {
	NodeID   string `json:"node_id"`
	RaftAddr string `json:"raft_addr"` // key used in mdNodeRegistry
}

// Propose serialises the given MetadataCommand and submits it to the
// Raft cluster via raft.Apply. It blocks until the entry is committed
// by a majority of nodes (or the 5-second timeout expires).
//
// This is the single chokepoint for all state-mutating operations.
// Only the Raft leader may call Propose; followers must reject writes
// and redirect clients to the current leader.
//
// The function checks both the Raft-level error (transport/timeout) and
// the FSM Apply() return value. If the FSM handler returns an error it
// is surfaced to the caller — otherwise mutations could be silently
// rejected while the gRPC handler reports success.
func Propose(raft *raft.Raft, cmd MetadataCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	// f.Response() returns the value from FSM.Apply(). By convention
	// our handlers return nil on success or an error on failure.
	if resp := f.Response(); resp != nil {
		if fsmErr, ok := resp.(error); ok {
			return fsmErr
		}
	}
	return nil
}
