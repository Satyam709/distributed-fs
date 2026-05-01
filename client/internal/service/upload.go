package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	"github.com/satyam709/distributed-fs/client/internal/chunker"
	"github.com/satyam709/distributed-fs/client/internal/dfsclientconfig"
	"github.com/satyam709/distributed-fs/client/internal/manifest"
	"github.com/satyam709/distributed-fs/client/internal/metadataclient"
	"github.com/satyam709/distributed-fs/client/internal/storageclient"
	"github.com/satyam709/distributed-fs/internal/checksum"
)

// UploadResult contains the outcome of an upload operation.
type UploadResult struct {
	FileID       string
	FileName     string
	TotalSize    int64
	ChunksTotal  int
	ChunksDone   int
	ChunksFailed int
	Error        error
}

// ProgressCallback is called as chunks complete (or fail).
// chunkIndex is the 0-based index, total is the total chunk count.
// err is nil on success; non-nil on failure.
type ProgressCallback func(chunkIndex int, total int, err error)

// UploadService orchestrates file uploads using chunker, manifest, metadata
// and storage clients. All business logic lives here — the CLI is just glue.
type UploadService struct {
	cfg       *dfsclientconfig.Config
	metadata  metadataclient.Client
	storage   storageclient.Client
	manifests *manifest.Manager
}

// NewUploadService creates an UploadService with all required dependencies.
func NewUploadService(
	cfg *dfsclientconfig.Config,
	meta metadataclient.Client,
	storage storageclient.Client,
	manifests *manifest.Manager,
) *UploadService {
	return &UploadService{
		cfg:       cfg,
		metadata:  meta,
		storage:   storage,
		manifests: manifests,
	}
}

// Upload performs the full upload flow:
//  1. Chunker → descriptors
//  2. Manifest create (or load for resume)
//  3. MetadataClient.CreateFile → placements
//  4. Parallel upload with semaphore
//  5. CommitChunk for each successful chunk
//  6. Manifest delete on full success
func (s *UploadService) Upload(ctx context.Context, filePath string, fileName string, progress ProgressCallback) (*UploadResult, error) {
	// --- Step 1: Generate chunk descriptors ---
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("upload: failed to stat file: %w", err)
	}

	descriptors, err := chunker.GenerateDescriptors(filePath, s.cfg.ChunkSize)
	if err != nil {
		return nil, fmt.Errorf("upload: failed to chunk file: %w", err)
	}

	if len(descriptors) == 0 {
		return nil, fmt.Errorf("upload: file %s is empty", filePath)
	}

	fileID := descriptors[0].FileID

	// --- Step 2: Manifest setup ---
	chunkIDs := make([]string, len(descriptors))
	chunkStatus := make(map[string]bool)
	for i, d := range descriptors {
		chunkIDs[i] = d.ChunkID
		chunkStatus[d.ChunkID] = false
	}

	m := &manifest.UploadManifest{
		FileID:      fileID,
		Filename:    fileName,
		TotalSize:   fileInfo.Size(),
		ChunkSize:   s.cfg.ChunkSize,
		ChunkStatus: chunkStatus,
	}

	if err := s.manifests.Create(m); err != nil {
		return nil, fmt.Errorf("upload: failed to create manifest: %w", err)
	}

	// --- Step 3: Register file with metadata service ---
	serverFileID, placements, err := s.metadata.CreateFile(ctx, fileName, fileInfo.Size(), s.cfg.ChunkSize, chunkIDs)
	if err != nil {
		return nil, fmt.Errorf("upload: CreateFile failed: %w", err)
	}
	_ = serverFileID // Server may assign different file_id; our manifest uses our local one

	// Build placement lookup: chunkID → Placement
	placementMap := make(map[string]metadataclient.Placement)
	for _, p := range placements {
		placementMap[p.ChunkID] = p
	}

	// --- Step 4: Open source file for concurrent reads ---
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("upload: failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// --- Step 5: Parallel upload ---
	var (
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, s.cfg.MaxParallelUploads)
		done      atomic.Int32
		failed    atomic.Int32
	)

	total := len(descriptors)

	for _, desc := range descriptors {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire slot

		go func(d chunker.ChunkDescriptor) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release slot

			err := s.uploadChunkWithRetry(ctx, file, d, placementMap[d.ChunkID])
			if err != nil {
				failed.Add(1)
				log.Printf("upload: chunk %d (%s) failed: %v", d.ChunkIndex, d.ChunkID, err)
				if progress != nil {
					progress(d.ChunkIndex, total, err)
				}
				return
			}

			// Mark done in manifest
			if markErr := s.manifests.MarkChunkDone(fileID, d.ChunkID); markErr != nil {
				log.Printf("upload: failed to update manifest for chunk %s: %v", d.ChunkID, markErr)
			}

			done.Add(1)
			if progress != nil {
				progress(d.ChunkIndex, total, nil)
			}
		}(desc)
	}

	wg.Wait()

	result := &UploadResult{
		FileID:       fileID,
		FileName:     fileName,
		TotalSize:    fileInfo.Size(),
		ChunksTotal:  total,
		ChunksDone:   int(done.Load()),
		ChunksFailed: int(failed.Load()),
	}

	if result.ChunksFailed > 0 {
		result.Error = fmt.Errorf("upload: %d/%d chunks failed", result.ChunksFailed, total)
		return result, result.Error
	}

	// --- Step 6: Cleanup manifest on success ---
	if err := s.manifests.Delete(fileID); err != nil {
		log.Printf("upload: failed to delete manifest: %v", err)
	}

	return result, nil
}

// uploadChunkWithRetry attempts to upload a single chunk with retries.
func (s *UploadService) uploadChunkWithRetry(ctx context.Context, file *os.File, desc chunker.ChunkDescriptor, placement metadataclient.Placement) error {
	// Read chunk bytes from file at the correct offset (ReadAt is thread-safe)
	buffer := make([]byte, desc.Size)
	n, err := file.ReadAt(buffer, desc.Offset)
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}
	buffer = buffer[:n]

	// Compute checksum before upload
	dataChecksum := checksum.Compute(buffer)

	// Build replicate_to list from placement
	var replicateTo []string
	replicateTo = append(replicateTo, placement.Replicas...)

	var lastErr error
	for attempt := 0; attempt < s.cfg.RetryAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := s.storage.PutChunk(ctx, placement.Primary, desc.ChunkID, desc.FileID, desc.ChunkIndex, buffer, dataChecksum, replicateTo)
		if err != nil {
			lastErr = err
			log.Printf("upload: chunk %s attempt %d/%d failed: %v", desc.ChunkID, attempt+1, s.cfg.RetryAttempts, err)
			continue
		}

		// Commit chunk to metadata
		confirmedNodes := append([]string{placement.Primary}, placement.Replicas...)
		if err := s.metadata.CommitChunk(ctx, desc.ChunkID, desc.FileID, confirmedNodes, dataChecksum); err != nil {
			lastErr = err
			log.Printf("upload: CommitChunk %s attempt %d/%d failed: %v", desc.ChunkID, attempt+1, s.cfg.RetryAttempts, err)
			continue
		}

		return nil // Success
	}

	return fmt.Errorf("all %d attempts failed for chunk %s: %w", s.cfg.RetryAttempts, desc.ChunkID, lastErr)
}
