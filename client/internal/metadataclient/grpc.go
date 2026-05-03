package metadataclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/leaderclient"
	"github.com/satyam709/distributed-fs/internal/retry"
)

// GRPCClient implements Client using the generated MetadataService gRPC stub
// backed by a LeaderAwareClient that handles leader redirection, address
// rotation, and retry transparently.
type GRPCClient struct {
	lc         *leaderclient.LeaderAwareClient
	metaClient pb_meta.MetadataServiceClient
	rpcTimeout time.Duration
}

// NewGRPCClient creates a metadata client that connects to the first
// reachable address from seedAddrs. The retry policy controls backoff
// behaviour for connectivity failures; leader redirects are followed
// immediately without consuming a retry attempt.
func NewGRPCClient(seedAddrs []string, rp retry.Policy, rpcTimeout time.Duration) (*GRPCClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lc, err := leaderclient.New(ctx, seedAddrs, rp)
	if err != nil {
		return nil, fmt.Errorf("metadataclient: %w", err)
	}
	return &GRPCClient{
		lc:         lc,
		metaClient: pb_meta.NewMetadataServiceClient(lc),
		rpcTimeout: rpcTimeout,
	}, nil
}

// Close shuts down the underlying leader-aware connection.
func (g *GRPCClient) Close() error {
	return g.lc.Close()
}

// CreateFile registers a new file with the metadata service and returns
// chunk placement assignments (primary + replicas for each chunk).
func (g *GRPCClient) CreateFile(ctx context.Context, fileID, fileName string, fileSize int64, chunkSize int64, chunkIDs []string) (string, []Placement, error) {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	resp, err := g.metaClient.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  fileName,
		FileSize:  fileSize,
		ChunkSize: chunkSize,
		ChunkIds:  chunkIDs,
	})
	if err != nil {
		return "", nil, fmt.Errorf("metadataclient: CreateFile RPC failed: %w", err)
	}

	placements := make([]Placement, 0, len(resp.GetPlacements()))
	for _, p := range resp.GetPlacements() {
		var replicas []string
		for _, r := range p.GetReplicas() {
			replicas = append(replicas, r.GetAddress())
		}
		placements = append(placements, Placement{
			ChunkID:  p.GetChunkId(),
			Primary:  p.GetPrimary().GetAddress(),
			Replicas: replicas,
		})
	}
	return resp.GetFileId(), placements, nil
}

// CommitChunk confirms that a chunk was successfully stored on the given
// nodes with the given checksum.
func (g *GRPCClient) CommitChunk(ctx context.Context, chunkID, fileID string, confirmedNodes []string, checksum []byte) error {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	_, err := g.metaClient.CommitChunk(ctx, &pb_meta.CommitChunkRequest{
		ChunkId:        chunkID,
		FileId:         fileID,
		ConfirmedNodes: confirmedNodes,
		Checksum:       checksum,
	})
	if err != nil {
		return fmt.Errorf("metadataclient: CommitChunk RPC failed: %w", err)
	}
	return nil
}

// GetFile returns file metadata and all chunk records with current replica
// locations for the given file ID.
func (g *GRPCClient) GetFile(ctx context.Context, fileID string) (*FileInfo, []ChunkInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	resp, err := g.metaClient.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})
	if err != nil {
		return nil, nil, fmt.Errorf("metadataclient: GetFile RPC failed: %w", err)
	}

	fi := protoToFileInfo(resp.GetFile())
	chunks := make([]ChunkInfo, 0, len(resp.GetChunks()))
	for _, c := range resp.GetChunks() {
		chunks = append(chunks, protoToChunkInfo(c))
	}
	return fi, chunks, nil
}

// GetFileByName finds a file by its human-readable name. Implemented by
// listing all files and filtering client-side (the current proto has no
// server-side name index).
func (g *GRPCClient) GetFileByName(ctx context.Context, fileName string) (*FileInfo, []ChunkInfo, error) {
	files, err := g.ListFiles(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	for _, f := range files {
		if f.FileName == fileName {
			return g.GetFile(ctx, f.FileID)
		}
	}
	return nil, nil, fmt.Errorf("metadataclient: file %q not found", fileName)
}

// ListFiles returns all files whose names start with the given prefix.
// Prefix filtering is performed client-side.
func (g *GRPCClient) ListFiles(ctx context.Context, prefix string) ([]FileInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	resp, err := g.metaClient.ListFiles(ctx, &pb_meta.ListFilesRequest{})
	if err != nil {
		return nil, fmt.Errorf("metadataclient: ListFiles RPC failed: %w", err)
	}

	files := make([]FileInfo, 0, len(resp.GetFiles()))
	for _, f := range resp.GetFiles() {
		if prefix != "" && !strings.HasPrefix(f.GetFileName(), prefix) {
			continue
		}
		files = append(files, *protoToFileInfo(f))
	}
	return files, nil
}

// DeleteFile marks a file as deleted in the metadata service.
func (g *GRPCClient) DeleteFile(ctx context.Context, fileID string) error {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	_, err := g.metaClient.DeleteFile(ctx, &pb_meta.DeleteFileRequest{FileId: fileID})
	if err != nil {
		return fmt.Errorf("metadataclient: DeleteFile RPC failed: %w", err)
	}
	return nil
}

// GetChunkLocations returns the gRPC addresses of live storage nodes
// holding replicas of the given chunk.
func (g *GRPCClient) GetChunkLocations(ctx context.Context, chunkID string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	resp, err := g.metaClient.GetChunkLocations(ctx, &pb_meta.GetChunkLocationsRequest{ChunkId: chunkID})
	if err != nil {
		return nil, fmt.Errorf("metadataclient: GetChunkLocations RPC failed: %w", err)
	}

	addrs := make([]string, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		addrs = append(addrs, n.GetAddress())
	}
	return addrs, nil
}

// protoToFileInfo converts a protobuf FileInfo message into the internal
// representation. Returns nil when the input is nil.
func protoToFileInfo(f *pb_meta.FileInfo) *FileInfo {
	if f == nil {
		return nil
	}
	return &FileInfo{
		FileID:    f.GetFileId(),
		FileName:  f.GetFileName(),
		FileSize:  f.GetFileSize(),
		ChunkSize: f.GetChunkSize(),
		ChunkIDs:  f.GetChunkIds(),
		Status:    f.GetStatus(),
		CreatedAt: f.GetCreatedAt(),
	}
}

// protoToChunkInfo converts a protobuf ChunkInfo message into the internal
// representation.
func protoToChunkInfo(c *pb_meta.ChunkInfo) ChunkInfo {
	return ChunkInfo{
		ChunkID:    c.GetChunkId(),
		FileID:     c.GetFileId(),
		ChunkIndex: int(c.GetChunkIndex()),
		Size:       c.GetSize(),
		Checksum:   c.GetChecksum(),
		Replicas:   c.GetReplicas(),
	}
}
