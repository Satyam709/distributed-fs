// client/uploader/uploader.go
package uploader

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/satyam709/distributed-fs/client/chunker"
	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/checksum"
)

// ParallelUploader orchestrates the full upload of one file.
type ParallelUploader struct {
	maxConcurrency int
	storageClient  pb_storage.StorageServiceClient // In a real app, this would be a pool.
}

func NewParallelUploader(concurrency int, client pb_storage.StorageServiceClient) *ParallelUploader {
	if concurrency <= 0 {
		concurrency = 4 // Default as per Section 6.
	}
	return &ParallelUploader{
		maxConcurrency: concurrency,
		storageClient:  client,
	}
}

// Upload handles the concurrent transfer of chunks.
func (u *ParallelUploader) Upload(ctx context.Context, filePath string, descriptors []chunker.ChunkDescriptor) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer file.Close()

	var wg sync.WaitGroup
	// Semaphore-limited goroutine pool.
	semaphore := make(chan struct{}, u.maxConcurrency)
	errChan := make(chan error, len(descriptors))

	for _, desc := range descriptors {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire slot
		fmt.Printf("Scheduling: ChunkID %s | Index: %d | Chunk Size: %d KB\n", desc.ChunkID, desc.ChunkIndex, desc.Size/1024)
		go func(d chunker.ChunkDescriptor) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release slot

			if err := u.uploadChunk(ctx, file, d); err != nil {
				errChan <- fmt.Errorf("chunk %d failed: %w", d.ChunkIndex, err)
			}
		}(desc)
	}

	wg.Wait()
	close(errChan)

	// Check if any goroutines reported errors
	if len(errChan) > 0 {
		return <-errChan 
	}

	return nil
}

func (u *ParallelUploader) uploadChunk(ctx context.Context, file *os.File, desc chunker.ChunkDescriptor) error {
	// Read bytes from file at the correct offset using ReadAt (thread-safe).
	if desc.Size < 0 || desc.Size > int64(int(^uint(0)>>1)) {
		return fmt.Errorf("chunk size %d is out of range for this platform", desc.Size)
	}
	bufferSize := int(desc.Size)
	buffer := make([]byte, bufferSize)
	_, err := file.ReadAt(buffer, desc.Offset)
	if err != nil {
		return err
	}

	// Compute checksum before upload.
	dataChecksum := checksum.Compute(buffer)

	// Thin wrapper around PutChunk gRPC.
	stream, err := u.storageClient.PutChunk(ctx)
	if err != nil {
		return err
	}

	// Framing logic: split chunk into 32KB frames.
	const frameSize = 32 * 1024
	for i := 0; i < len(buffer); i += frameSize {
		end := i + frameSize
		end = min(end, len(buffer))

		fmt.Println("frame no : ",i/32768,"chunk index : ", desc.ChunkIndex)

		req := &pb_storage.PutChunkRequest{
			ChunkId:  desc.ChunkID,
			FileId:   desc.FileID,
			Data:     buffer[i:end],
			Checksum: []byte(dataChecksum),
			IsLast:   end == len(buffer),
		}
		
		if err := stream.Send(req); err != nil {
			return err
		}
	}
	
	_, err = stream.CloseAndRecv()
	fmt.Println("```````````````````````````````````````````````````````````````````````````````````````````````````")
	return err
}