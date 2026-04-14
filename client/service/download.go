package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/satyam709/distributed-fs/client"
	"github.com/satyam709/distributed-fs/client/metadataclient"
	"github.com/satyam709/distributed-fs/client/storageclient"
	"github.com/satyam709/distributed-fs/internal/checksum"
)

// DownloadResult contains the outcome of a download operation.
type DownloadResult struct {
	FileID     string
	FileName   string
	TotalSize  int64
	OutputPath string
	Error      error
}

// DownloadService orchestrates file downloads using metadata and storage clients.
type DownloadService struct {
	cfg      *client.Config
	metadata metadataclient.Client
	storage  storageclient.Client
}

// NewDownloadService creates a DownloadService with all required dependencies.
func NewDownloadService(
	cfg *client.Config,
	meta metadataclient.Client,
	storage storageclient.Client,
) *DownloadService {
	return &DownloadService{
		cfg:      cfg,
		metadata: meta,
		storage:  storage,
	}
}

// Download performs the full download flow:
//  1. GetFile from metadata → file info + chunk list with replica addresses
//  2. Pre-allocate output file to full size
//  3. Parallel download with semaphore (N concurrent)
//  4. Checksum verification per chunk
//  5. Replica failover on error
func (s *DownloadService) Download(ctx context.Context, fileName string, outputPath string, progress ProgressCallback) (*DownloadResult, error) {
	// --- Step 1: Get file metadata ---
	fileInfo, chunks, err := s.metadata.GetFileByName(ctx, fileName)
	if err != nil {
		return nil, fmt.Errorf("download: failed to get file metadata: %w", err)
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("download: file %q has no chunks", fileName)
	}

	// Sort chunks by index to ensure correct assembly
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].ChunkIndex < chunks[j].ChunkIndex
	})

	// --- Step 2: Pre-allocate output file ---
	outFile, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("download: failed to create output file: %w", err)
	}
	defer outFile.Close()

	if err := outFile.Truncate(fileInfo.FileSize); err != nil {
		return nil, fmt.Errorf("download: failed to pre-allocate file: %w", err)
	}

	// --- Step 3: Parallel download ---
	var (
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, s.cfg.MaxParallelDownloads)
		done      atomic.Int32
		failed    atomic.Int32
		errOnce   sync.Once
		firstErr  error
	)

	total := len(chunks)

	for _, chunk := range chunks {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire slot

		go func(c metadataclient.ChunkInfo) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release slot

			err := s.downloadChunkWithFailover(ctx, outFile, c, fileInfo.ChunkSize)
			if err != nil {
				failed.Add(1)
				errOnce.Do(func() { firstErr = err })
				log.Printf("download: chunk %d (%s) failed: %v", c.ChunkIndex, c.ChunkID, err)
				if progress != nil {
					progress(c.ChunkIndex, total, err)
				}
				return
			}

			done.Add(1)
			if progress != nil {
				progress(c.ChunkIndex, total, nil)
			}
		}(chunk)
	}

	wg.Wait()

	result := &DownloadResult{
		FileID:     fileInfo.FileID,
		FileName:   fileInfo.FileName,
		TotalSize:  fileInfo.FileSize,
		OutputPath: outputPath,
	}

	if failed.Load() > 0 {
		result.Error = fmt.Errorf("download: %d/%d chunks failed: %w", failed.Load(), total, firstErr)
		// Clean up partial file on failure
		outFile.Close()
		os.Remove(outputPath)
		return result, result.Error
	}

	return result, nil
}

// downloadChunkWithFailover tries each replica in order until one succeeds.
// Verifies checksum after download if available.
func (s *DownloadService) downloadChunkWithFailover(ctx context.Context, outFile *os.File, chunk metadataclient.ChunkInfo, chunkSize int64) error {
	if len(chunk.Replicas) == 0 {
		return fmt.Errorf("no replicas available for chunk %s", chunk.ChunkID)
	}

	var lastErr error
	for _, addr := range chunk.Replicas {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		data, err := s.storage.GetChunk(ctx, addr, chunk.ChunkID)
		if err != nil {
			lastErr = err
			log.Printf("download: replica %s failed for chunk %s, trying next: %v", addr, chunk.ChunkID, err)
			continue
		}

		// Verify checksum if we have one from metadata
		if chunk.Checksum != "" {
			computed := checksum.Compute(data)
			if computed != chunk.Checksum {
				lastErr = fmt.Errorf("checksum mismatch for chunk %s from %s: got %s, want %s",
					chunk.ChunkID, addr, computed, chunk.Checksum)
				log.Printf("download: %v", lastErr)
				continue
			}
		}

		// Write to correct offset (thread-safe, no coordination needed)
		offset := int64(chunk.ChunkIndex) * chunkSize
		if _, err := outFile.WriteAt(data, offset); err != nil {
			return fmt.Errorf("failed to write chunk %s at offset %d: %w", chunk.ChunkID, offset, err)
		}

		return nil // Success
	}

	return fmt.Errorf("all replicas failed for chunk %s (index %d): %w", chunk.ChunkID, chunk.ChunkIndex, lastErr)
}
