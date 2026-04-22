package fsm

import "time"

// FileStatus represents the lifecycle state of a file.
type FileStatus string

const (
	FileStatusCreating FileStatus = "creating"
	FileStatusComplete FileStatus = "complete"
	FileStatusDeleted  FileStatus = "deleted"
)

// ChunkStatus represents the lifecycle state of a chunk.
type ChunkStatus string

const (
	ChunkStatusAllocated ChunkStatus = "allocated"
	ChunkStatusComplete  ChunkStatus = "complete"
	ChunkStatusLost      ChunkStatus = "lost"
)

// NodeStatus represents the health state of a storage node.
type NodeStatus string

const (
	NodeStatusAlive    NodeStatus = "alive"
	NodeStatusSuspect  NodeStatus = "suspect"
	NodeStatusDead     NodeStatus = "dead"
	NodeStatusDraining NodeStatus = "draining"
)

// RepairStatus represents the lifecycle state of a repair job.
type RepairStatus string

const (
	RepairStatusPending    RepairStatus = "pending"
	RepairStatusInProgress RepairStatus = "in-progress"
	RepairStatusDone       RepairStatus = "done"
	RepairStatusFailed     RepairStatus = "failed"
)

// FileRecord represents a file tracked by the metadata service.
// ChunkIDs is ordered — the slice index equals the chunk_index.
type FileRecord struct {
	FileID    string     `json:"file_id"`
	Filename  string     `json:"filename"`
	FileSize  uint64     `json:"file_size"`
	ChunkSize uint64     `json:"chunk_size"`
	CheckSum  []byte     `json:"checksum"`
	ChunkIDs  []string   `json:"chunk_ids"`
	Status    FileStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// ChunkRecord represents a single chunk and the set of storage nodes
// that hold replicas of it.
type ChunkRecord struct {
	ChunkID    string      `json:"chunk_id"`
	FileID     string      `json:"file_id"`
	ChunkIndex int         `json:"chunk_index"`
	Size       int64       `json:"size"`
	Checksum   []byte      `json:"checksum"`
	Replicas   []string    `json:"replicas"` // node IDs holding this chunk
	Status     ChunkStatus `json:"status"`
	Version    int64       `json:"version"`
}

// NodeEntry represents a storage node as known by the metadata service.
// LastSeen is maintained in-memory only (updated by heartbeats) and is
// NOT replicated through Raft.
type NodeEntry struct {
	NodeID       string     `json:"node_id"`
	Address      string     `json:"address"`
	Status       NodeStatus `json:"status"`
	FreeSpace    uint64     `json:"free_space"`
	ChunkCount   uint64     `json:"chunk_count"`
	LastSeen     time.Time  `json:"-"` // in-memory only, not persisted via Raft
	RegisteredAt time.Time  `json:"registered_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// RepairJob represents one unit of chunk-repair work.
type RepairJob struct {
	JobID        string       `json:"job_id"`
	ChunkID      string       `json:"chunk_id"`
	Error        string       `json:"error"`
	DeleteSource bool         `json:"delete_source"`
	SourceNodeID string       `json:"source_node_id"`
	TargetNodeID string       `json:"target_node_id"`
	Status       RepairStatus `json:"status"`
	Attempts     uint64       `json:"attempts"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
}
