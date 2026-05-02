package metadataclient

import (
	"context"
	"fmt"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/leaderclient"
	"github.com/satyam709/distributed-fs/internal/retry"
)

const metadataServicePath = "/proto.metadata.v1.MetadataService/"

type GRPCClient struct {
	client     *leaderclient.LeaderAwareClient
	rpcTimeout time.Duration
}

func NewGRPCClient(seedAddrs []string, rp retry.Policy, rpcTimeout time.Duration) (*GRPCClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lc, err := leaderclient.New(ctx, seedAddrs, rp)
	if err != nil {
		return nil, fmt.Errorf("metadataclient: %w", err)
	}
	return &GRPCClient{client: lc, rpcTimeout: rpcTimeout}, nil
}

func (g *GRPCClient) Close() error {
	return g.client.Close()
}

func (g *GRPCClient) CreateFile(ctx context.Context, fileName string, fileSize int64, chunkSize int64, chunkIDs []string) (string, []Placement, error) {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	req := &pb_meta.CreateFileRequest{
		FileName:  fileName,
		FileSize:  fileSize,
		ChunkSize: chunkSize,
		ChunkIds:  chunkIDs,
	}
	var resp pb_meta.CreateFileResponse
	if err := g.client.Invoke(ctx, metadataServicePath+"CreateFile", req, &resp); err != nil {
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
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	req := &pb_meta.CommitChunkRequest{
		ChunkId:        chunkID,
		FileId:         fileID,
		ConfirmedNodes: confirmedNodes,
		Checksum:       []byte(checksum),
	}
	var resp pb_meta.CommitChunkResponse
	if err := g.client.Invoke(ctx, metadataServicePath+"CommitChunk", req, &resp); err != nil {
		return fmt.Errorf("metadataclient: CommitChunk RPC failed: %w", err)
	}
	return nil
}

func (g *GRPCClient) GetFile(ctx context.Context, fileID string) (*FileInfo, []ChunkInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	req := &pb_meta.GetFileRequest{FileId: fileID}
	var resp pb_meta.GetFileResponse
	if err := g.client.Invoke(ctx, metadataServicePath+"GetFile", req, &resp); err != nil {
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
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	req := &pb_meta.ListFilesRequest{}
	var resp pb_meta.ListFilesResponse
	if err := g.client.Invoke(ctx, metadataServicePath+"ListFiles", req, &resp); err != nil {
		return nil, fmt.Errorf("metadataclient: ListFiles RPC failed: %w", err)
	}

	files := make([]FileInfo, 0, len(resp.GetFiles()))
	for _, f := range resp.GetFiles() {
		files = append(files, *protoToFileInfo(f))
	}
	return files, nil
}

func (g *GRPCClient) DeleteFile(ctx context.Context, fileID string) error {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	req := &pb_meta.DeleteFileRequest{FileId: fileID}
	var resp pb_meta.DeleteFileResponse
	if err := g.client.Invoke(ctx, metadataServicePath+"DeleteFile", req, &resp); err != nil {
		return fmt.Errorf("metadataclient: DeleteFile RPC failed: %w", err)
	}
	return nil
}

func (g *GRPCClient) GetChunkLocations(ctx context.Context, chunkID string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, g.rpcTimeout)
	defer cancel()

	req := &pb_meta.GetChunkLocationsRequest{ChunkId: chunkID}
	var resp pb_meta.GetChunkLocationsResponse
	if err := g.client.Invoke(ctx, metadataServicePath+"GetChunkLocations", req, &resp); err != nil {
		return nil, fmt.Errorf("metadataclient: GetChunkLocations RPC failed: %w", err)
	}

	addrs := make([]string, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		addrs = append(addrs, n.GetAddress())
	}
	return addrs, nil
}

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
		Checksum:   string(c.GetChecksum()),
		Replicas:   c.GetReplicas(),
	}
}
