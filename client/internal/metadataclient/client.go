// Package metadataclient provides an interface for interacting with the
// metadata service. Business logic depends on the Client interface, not
// on gRPC stubs directly — enabling mock-based testing and easy backend swap.
package metadataclient

import "context"

// Placement represents where a chunk should be stored (primary + replicas).
type Placement struct {
	ChunkID  string
	Primary  string   // address of the primary node
	Replicas []string // addresses of replica nodes
}

// FileInfo represents file metadata returned by the metadata service.
type FileInfo struct {
	FileID    string
	FileName  string
	FileSize  int64
	ChunkSize int64
	ChunkIDs  []string
	Status    string
	CreatedAt int64
}

// ChunkInfo represents chunk metadata with replica location addresses.
type ChunkInfo struct {
	ChunkID    string
	FileID     string
	ChunkIndex int
	Size       int64
	Checksum   []byte
	Replicas   []string // node addresses that hold this chunk
}

// Client defines the metadata operations the DFS client needs.
// Implementations include a gRPC client (for production) and an in-memory
// mock (for testing).
type Client interface {
	// CreateFile registers a new file and obtains chunk placement assignments.
	// Returns the server-assigned file_id and a placement map for each chunk.
	CreateFile(ctx context.Context, fileID, fileName string, fileSize int64, chunkSize int64, chunkIDs []string) (string, []Placement, error)

	// CommitChunk tells metadata that a chunk has been successfully stored
	// on the given nodes with the given checksum.
	CommitChunk(ctx context.Context, chunkID, fileID string, confirmedNodes []string, checksum []byte) error

	// GetFile retrieves the metadata for a file including all its chunks
	// and current replica locations.
	GetFile(ctx context.Context, fileID string) (*FileInfo, []ChunkInfo, error)

	// GetFileByName retrieves file metadata by filename. This is a
	// convenience that the CLI uses (users reference files by name).
	GetFileByName(ctx context.Context, fileName string) (*FileInfo, []ChunkInfo, error)

	// ListFiles returns all files, optionally filtered by a name prefix.
	ListFiles(ctx context.Context, prefix string) ([]FileInfo, error)

	// DeleteFile marks a file as deleted in the metadata.
	DeleteFile(ctx context.Context, fileID string) error

	// CommitFile finalises a file upload by transitioning its status from
	// "creating" to "complete" after validating all chunks are committed.
	CommitFile(ctx context.Context, fileID string, fileSize int64, checksum []byte) error

	// GetChunkLocations returns the live node addresses for a chunk.
	GetChunkLocations(ctx context.Context, chunkID string) ([]string, error)

	// Close cleans up any resources held by the client (e.g. gRPC connections).
	Close() error
}
