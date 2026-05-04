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

type UploadResult struct {
	FileID       string
	FileName     string
	TotalSize    int64
	ChunksTotal  int
	ChunksDone   int
	ChunksFailed int
	Error        error
}

type ProgressCallback func(chunkIndex int, total int, err error)

type UploadService struct {
	cfg       *dfsclientconfig.Config
	metadata  metadataclient.Client
	storage   storageclient.Client
	manifests *manifest.Manager
}

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

func (s *UploadService) Upload(ctx context.Context, filePath string, fileName string, progress ProgressCallback) (*UploadResult, error) {
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

	serverFileID, placements, err := s.metadata.CreateFile(ctx, fileID, fileName, fileInfo.Size(), s.cfg.ChunkSize, chunkIDs)
	if err != nil {
		return nil, fmt.Errorf("upload: CreateFile failed: %w", err)
	}

	placementMap := make(map[string]metadataclient.Placement)
	for _, p := range placements {
		placementMap[p.ChunkID] = p
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("upload: failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	var (
		wg        sync.WaitGroup
		semaphore = make(chan struct{}, s.cfg.MaxParallelUploads)
		done      atomic.Int32
		failed    atomic.Int32
	)

	total := len(descriptors)

	for _, desc := range descriptors {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(d chunker.ChunkDescriptor) {
			defer wg.Done()
			defer func() { <-semaphore }()

			err := s.uploadChunkWithRetry(ctx, file, d, placementMap[d.ChunkID])
			if err != nil {
				failed.Add(1)
				log.Printf("upload: chunk %d (%s) failed: %v", d.ChunkIndex, d.ChunkID, err)
				if progress != nil {
					progress(d.ChunkIndex, total, err)
				}
				return
			}

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
		FileID:       serverFileID,
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

	if err := s.metadata.CommitFile(ctx, serverFileID, fileInfo.Size(), nil); err != nil {
		result.Error = fmt.Errorf("upload: CommitFile failed: %w", err)
		return result, result.Error
	}

	if err := s.manifests.Delete(fileID); err != nil {
		log.Printf("upload: failed to delete manifest: %v", err)
	}

	return result, nil
}

func (s *UploadService) uploadChunkWithRetry(ctx context.Context, file *os.File, desc chunker.ChunkDescriptor, placement metadataclient.Placement) error {
	buffer := make([]byte, desc.Size)
	n, err := file.ReadAt(buffer, desc.Offset)
	if err != nil {
		return fmt.Errorf("failed to read chunk data: %w", err)
	}
	buffer = buffer[:n]

	dataChecksum := checksum.Compute(buffer)

	var replicateTo []string
	replicateTo = append(replicateTo, placement.Replicas...)

	err = s.cfg.RetryPolicy().Do(ctx, func() error {
		if err := s.storage.PutChunk(ctx, placement.Primary, desc.ChunkID, desc.FileID, desc.ChunkIndex, buffer, dataChecksum, replicateTo); err != nil {
			log.Printf("upload: chunk %s PutChunk failed: %v", desc.ChunkID, err)
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("uploadChunkWithRetry chunk=%s primary=%s: %w", desc.ChunkID, placement.Primary, err)
	}
	return nil
}
