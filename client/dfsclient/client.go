package dfsclient

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/satyam709/distributed-fs/client/internal/dfsclientconfig"
	"github.com/satyam709/distributed-fs/client/internal/manifest"
	"github.com/satyam709/distributed-fs/client/internal/metadataclient"
	"github.com/satyam709/distributed-fs/client/internal/service"
	"github.com/satyam709/distributed-fs/client/internal/storageclient"
)

// Client is the main entrypoint for the DFS SDK.
// It manages gRPC connections, chunking, manifests, and provides a
// clean API for file operations on the distributed file system.
//
// A Client is safe for concurrent use by multiple goroutines.
type Client struct {
	cfg       *dfsclientconfig.Config
	metadata  metadataclient.Client
	storage   storageclient.Client
	manifests *manifest.Manager
}

// New creates a new DFS client with the given options.
// The returned Client must be closed with Close() when no longer needed.
//
//	c, err := dfsclient.New(
//	    dfsclient.WithMetadataAddrs("meta-1:50050", "meta-2:50050"),
//	    dfsclient.WithChunkSize(8 * 1024 * 1024),
//	)
func New(opts ...Option) (*Client, error) {
	cfg := dfsclientconfig.DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	if len(cfg.MetadataAddrs) == 0 {
		return nil, fmt.Errorf("dfsclient: at least one metadata address is required")
	}

	meta, err := metadataclient.NewGRPCClient(cfg.MetadataAddrs[0])
	if err != nil {
		return nil, fmt.Errorf("dfsclient: failed to connect to metadata service: %w", err)
	}

	storage := storageclient.NewGRPCClient(cfg.FrameSize)
	mgr := manifest.NewManager(cfg.ManifestDir)

	return &Client{
		cfg:       cfg,
		metadata:  meta,
		storage:   storage,
		manifests: mgr,
	}, nil
}

// newFromDeps creates a Client with injected dependencies (for testing).
func newFromDeps(cfg *dfsclientconfig.Config, meta metadataclient.Client, storage storageclient.Client, mgr *manifest.Manager) *Client {
	return &Client{
		cfg:       cfg,
		metadata:  meta,
		storage:   storage,
		manifests: mgr,
	}
}

// Upload uploads a local file at filePath to the distributed FS under remoteName.
// If progress is non-nil, it is called after each chunk completes or fails.
//
//	result, err := c.Upload(ctx, "./data.bin", "data.bin", func(info dfsclient.ProgressInfo) {
//	    fmt.Printf("%.1f%% complete\n", info.Percent())
//	})
func (c *Client) Upload(ctx context.Context, filePath, remoteName string, progress ProgressFunc) (*UploadResult, error) {
	svc := service.NewUploadService(c.cfg, c.metadata, c.storage, c.manifests)

	// Adapt the rich ProgressFunc to the internal service callback.
	var bytesDone atomic.Int64
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("dfsclient: failed to stat file: %w", err)
	}
	totalBytes := fileInfo.Size()

	var internalProgress service.ProgressCallback
	if progress != nil {
		internalProgress = func(chunkIndex, total int, chunkErr error) {
			// Estimate bytes done based on chunk index (approximate, since last chunk may be smaller)
			chunkSize := c.cfg.ChunkSize
			if chunkErr == nil {
				done := bytesDone.Add(chunkSize)
				if done > totalBytes {
					done = totalBytes
				}
				bytesDone.Store(done)
			}
			progress(ProgressInfo{
				ChunkIndex:  chunkIndex,
				ChunksTotal: total,
				BytesDone:   bytesDone.Load(),
				BytesTotal:  totalBytes,
				Err:         chunkErr,
			})
		}
	}

	result, err := svc.Upload(ctx, filePath, remoteName, internalProgress)
	if err != nil {
		// Still return partial result when available
		if result != nil {
			return &UploadResult{
				FileID:       result.FileID,
				FileName:     result.FileName,
				TotalSize:    result.TotalSize,
				ChunksTotal:  result.ChunksTotal,
				ChunksDone:   result.ChunksDone,
				ChunksFailed: result.ChunksFailed,
			}, err
		}
		return nil, err
	}

	return &UploadResult{
		FileID:       result.FileID,
		FileName:     result.FileName,
		TotalSize:    result.TotalSize,
		ChunksTotal:  result.ChunksTotal,
		ChunksDone:   result.ChunksDone,
		ChunksFailed: result.ChunksFailed,
	}, nil
}

// UploadReader uploads data from an io.Reader to the distributed FS.
// This is useful for HTTP request bodies, in-memory buffers, and other
// streaming sources where no local file exists.
//
// The data is first written to a temporary file (since chunking requires
// random access), then uploaded normally, and the temp file is cleaned up.
//
//	result, err := c.UploadReader(ctx, req.Body, req.ContentLength, "upload.bin", nil)
func (c *Client) UploadReader(ctx context.Context, r io.Reader, size int64, remoteName string, progress ProgressFunc) (*UploadResult, error) {
	// Write to a temp file — chunking requires random access via ReadAt.
	tmpDir := filepath.Join(c.cfg.ManifestDir, ".tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("dfsclient: failed to create temp dir: %w", err)
	}

	tmpFile, err := os.CreateTemp(tmpDir, "dfs-upload-*")
	if err != nil {
		return nil, fmt.Errorf("dfsclient: failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmpFile, r); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("dfsclient: failed to buffer upload data: %w", err)
	}
	tmpFile.Close()

	return c.Upload(ctx, tmpPath, remoteName, progress)
}

// Download downloads a file from the distributed FS to the local outputPath.
// If progress is non-nil, it is called after each chunk completes or fails.
//
//	result, err := c.Download(ctx, "data.bin", "./downloads/data.bin", nil)
func (c *Client) Download(ctx context.Context, remoteName, outputPath string, progress ProgressFunc) (*DownloadResult, error) {
	svc := service.NewDownloadService(c.cfg, c.metadata, c.storage)

	var internalProgress service.ProgressCallback
	if progress != nil {
		var bytesDone atomic.Int64
		internalProgress = func(chunkIndex, total int, chunkErr error) {
			if chunkErr == nil {
				bytesDone.Add(c.cfg.ChunkSize)
			}
			progress(ProgressInfo{
				ChunkIndex:  chunkIndex,
				ChunksTotal: total,
				BytesDone:   bytesDone.Load(),
				BytesTotal:  0, // We don't know total until metadata returns it
				Err:         chunkErr,
			})
		}
	}

	result, err := svc.Download(ctx, remoteName, outputPath, internalProgress)
	if err != nil {
		if result != nil {
			return &DownloadResult{
				FileID:     result.FileID,
				FileName:   result.FileName,
				TotalSize:  result.TotalSize,
				OutputPath: result.OutputPath,
			}, err
		}
		return nil, err
	}

	return &DownloadResult{
		FileID:     result.FileID,
		FileName:   result.FileName,
		TotalSize:  result.TotalSize,
		OutputPath: result.OutputPath,
	}, nil
}

// List returns files stored in the distributed FS.
// If prefix is non-empty, only files whose names start with prefix are returned.
//
//	files, err := c.List(ctx, "reports/")
func (c *Client) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	svc := service.NewListService(c.metadata)

	internalFiles, err := svc.ListFiles(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("dfsclient: %w", err)
	}

	files := make([]FileInfo, len(internalFiles))
	for i, f := range internalFiles {
		files[i] = FileInfo{
			FileID:     f.FileID,
			FileName:   f.FileName,
			FileSize:   f.FileSize,
			ChunkSize:  f.ChunkSize,
			ChunkCount: len(f.ChunkIDs),
			Status:     f.Status,
			CreatedAt:  f.CreatedAt,
		}
	}
	return files, nil
}

// Delete deletes a file from the distributed FS by its file ID.
//
//	err := c.Delete(ctx, "file-001")
func (c *Client) Delete(ctx context.Context, fileID string) error {
	svc := service.NewListService(c.metadata)

	if err := svc.DeleteFile(ctx, fileID); err != nil {
		return fmt.Errorf("dfsclient: %w", err)
	}
	return nil
}

// Close cleans up all connections and resources held by the client.
// Must be called when the client is no longer needed.
func (c *Client) Close() error {
	var firstErr error

	if err := c.metadata.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("dfsclient: failed to close metadata connection: %w", err)
	}
	if err := c.storage.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("dfsclient: failed to close storage connections: %w", err)
	}

	return firstErr
}
