package metadataclient

import (
	"context"
	"fmt"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient implements Client using the generated MetadataService gRPC stub.
// It hides raw proto types from the rest of the client.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client pb_meta.MetadataServiceClient
}

// NewGRPCClient connects to the metadata service at the given address.
func NewGRPCClient(addr string) (*GRPCClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("metadataclient: failed to connect to %s: %w", addr, err)
	}
	return &GRPCClient{
		conn:   conn,
		client: pb_meta.NewMetadataServiceClient(conn),
	}, nil
}

// Close shuts down the gRPC connection.
func (g *GRPCClient) Close() error {
	return g.conn.Close()
}

func (g *GRPCClient) CreateFile(ctx context.Context, fileName string, fileSize int64, chunkSize int64, chunkIDs []string) (string, []Placement, error) {
	resp, err := g.client.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileName: fileName,
		FileSize: fileSize,
		ChunkSize: chunkSize,
		ChunkIds: chunkIDs,
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

func (g *GRPCClient) CommitChunk(ctx context.Context, chunkID, fileID string, confirmedNodes []string, checksum string) error {
	_, err := g.client.CommitChunk(ctx, &pb_meta.CommitChunkRequest{
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

func (g *GRPCClient) GetFile(ctx context.Context, fileID string) (*FileInfo, []ChunkInfo, error) {
	resp, err := g.client.GetFile(ctx, &pb_meta.GetFileRequest{
		FileId: fileID,
	})
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

func (g *GRPCClient) GetFileByName(ctx context.Context, fileName string) (*FileInfo, []ChunkInfo, error) {
	// The proto GetFile takes file_id. To search by name, we list and filter.
	// A future proto update could add a GetFileByName RPC. For now, use ListFiles.
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

func (g *GRPCClient) ListFiles(ctx context.Context, prefix string) ([]FileInfo, error) {
	resp, err := g.client.ListFiles(ctx, &pb_meta.ListFilesRequest{
		Prefix: prefix,
	})
	if err != nil {
		return nil, fmt.Errorf("metadataclient: ListFiles RPC failed: %w", err)
	}

	files := make([]FileInfo, 0, len(resp.GetFiles()))
	for _, f := range resp.GetFiles() {
		files = append(files, *protoToFileInfo(f))
	}
	return files, nil
}

func (g *GRPCClient) DeleteFile(ctx context.Context, fileID string) error {
	_, err := g.client.DeleteFile(ctx, &pb_meta.DeleteFileRequest{
		FileId: fileID,
	})
	if err != nil {
		return fmt.Errorf("metadataclient: DeleteFile RPC failed: %w", err)
	}
	return nil
}

func (g *GRPCClient) GetChunkLocations(ctx context.Context, chunkID string) ([]string, error) {
	resp, err := g.client.GetChunkLocations(ctx, &pb_meta.GetChunkLocationsRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return nil, fmt.Errorf("metadataclient: GetChunkLocations RPC failed: %w", err)
	}

	addrs := make([]string, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		addrs = append(addrs, n.GetAddress())
	}
	return addrs, nil
}

// --- proto conversion helpers ---

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
