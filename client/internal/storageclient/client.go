// Package storageclient provides an interface for interacting with storage
// nodes. Business logic depends on the Client interface, not on gRPC stubs.
package storageclient

import "context"

// Client defines storage node operations.
// The addr parameter is passed per-call — the implementation manages
// connection pooling internally.
type Client interface {
	// PutChunk uploads a chunk to a storage node. The implementation handles
	// framing (splitting data into 32KB frames) and streaming.
	// replicateTo contains addresses of nodes the primary should replicate to.
	PutChunk(ctx context.Context, addr string, chunkID, fileID string, chunkIndex int, data []byte, checksum []byte, replicateTo []string) error

	// GetChunk downloads a chunk from a storage node. The implementation
	// handles stream frame assembly.
	GetChunk(ctx context.Context, addr string, chunkID string) ([]byte, error)

	// Close cleans up all pooled connections.
	Close() error
}
