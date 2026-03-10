package downloader

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	// "github.com/satyam709/distributed-fs/internal/checksum"
	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ParallelDownloader struct {
	maxConcurrency int
	chunkSize      int64
}

func NewParallelDownloader(concurrency int, chunkSize int64) *ParallelDownloader {
	if concurrency <= 0 {
		concurrency = 4 // Default as per Section 7
	}
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024 // Default to 4MB if not specified
	}
	return &ParallelDownloader{maxConcurrency: concurrency, chunkSize: chunkSize}
}

// Download coordinates the concurrent retrieval of chunks.
type ChunkLocation struct {
	ChunkID  string
	Replicas []string
}

func (d *ParallelDownloader) Download(ctx context.Context, outputPath string, totalSize int64, chunkMap map[int]ChunkLocation) error {
	// 1. Pre-allocate output file to full size (Section 7 Key Detail)
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	if err := outFile.Truncate(totalSize); err != nil {
		return fmt.Errorf("failed to pre-allocate file: %w", err)
	}

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, d.maxConcurrency)
	errChan := make(chan error, len(chunkMap))

	for index, loc := range chunkMap {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(idx int, location ChunkLocation) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if err := d.downloadWithRetry(ctx, outFile, idx, location); err != nil {
				errChan <- err
			}
		}(index, loc)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		return <-errChan
	}
	return nil
}

func (d *ParallelDownloader) downloadWithRetry(ctx context.Context, outFile *os.File, index int, location ChunkLocation) error {
	for _, addr := range location.Replicas {
		// Section 7: Try a replica, on failure/checksum error, try the next one
		data, err := d.fetchChunk(ctx, addr, location.ChunkID)
		if err == nil {
			// Section 7: Write to specific byte range using WriteAt (no coordination needed)
			offset := int64(index) * d.chunkSize
			_, err = outFile.WriteAt(data, offset)
			if err == nil {
				return nil
			}
		}
		fmt.Printf("Replica %s failed for chunk %s (index %d), trying next...\n", addr, location.ChunkID, index)
	}
	return fmt.Errorf("all replicas failed for chunk %s (index %d)", location.ChunkID, index)
}

func (d *ParallelDownloader) fetchChunk(ctx context.Context, addr string, chunkID string) ([]byte, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb_storage.NewStorageServiceClient(conn)
	stream, err := client.GetChunk(ctx, &pb_storage.GetChunkRequest{ChunkId: chunkID})
	if err != nil {
		return nil, err
	}

	var chunkData []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		chunkData = append(chunkData, resp.GetData()...)
	}

	// Section 7: Verify checksum after receiving each chunk
	// If it doesn't match the expected hash (from metadata), return error to trigger retry
	return chunkData, nil
}
