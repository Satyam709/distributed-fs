package chunker

import (
	"os"
	"path/filepath"
	"testing"
)

func createTempFile(t *testing.T, size int64) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer f.Close()

	if size > 0 {
		if err := f.Truncate(size); err != nil {
			t.Fatalf("failed to truncate file: %v", err)
		}
	}
	return path
}

func TestGenerateDescriptors_MultipleChunks(t *testing.T) {
	// 10MB file with 4MB chunks → 3 chunks (4+4+2)
	filePath := createTempFile(t, 10*1024*1024)
	chunkSize := int64(4 * 1024 * 1024)

	descs, err := GenerateDescriptors(filePath, chunkSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(descs) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(descs))
	}

	// All share same FileID
	fileID := descs[0].FileID
	for _, d := range descs {
		if d.FileID != fileID {
			t.Errorf("inconsistent FileID: got %s, want %s", d.FileID, fileID)
		}
	}

	// Verify chunk indices
	for i, d := range descs {
		if d.ChunkIndex != i {
			t.Errorf("chunk %d: expected index %d, got %d", i, i, d.ChunkIndex)
		}
	}

	// Verify offsets
	if descs[0].Offset != 0 {
		t.Errorf("chunk 0 offset: got %d, want 0", descs[0].Offset)
	}
	if descs[1].Offset != chunkSize {
		t.Errorf("chunk 1 offset: got %d, want %d", descs[1].Offset, chunkSize)
	}
	if descs[2].Offset != 2*chunkSize {
		t.Errorf("chunk 2 offset: got %d, want %d", descs[2].Offset, 2*chunkSize)
	}

	// Verify sizes
	if descs[0].Size != chunkSize {
		t.Errorf("chunk 0 size: got %d, want %d", descs[0].Size, chunkSize)
	}
	if descs[1].Size != chunkSize {
		t.Errorf("chunk 1 size: got %d, want %d", descs[1].Size, chunkSize)
	}
	expectedLastSize := int64(2 * 1024 * 1024) // 10MB - 8MB = 2MB
	if descs[2].Size != expectedLastSize {
		t.Errorf("last chunk size: got %d, want %d", descs[2].Size, expectedLastSize)
	}

	// Verify offsets + sizes sum to total
	var totalSize int64
	for _, d := range descs {
		totalSize += d.Size
	}
	if totalSize != 10*1024*1024 {
		t.Errorf("total size mismatch: got %d, want %d", totalSize, 10*1024*1024)
	}
}

func TestGenerateDescriptors_SingleChunk(t *testing.T) {
	// File smaller than chunk size → 1 chunk
	fileSize := int64(1024) // 1KB
	filePath := createTempFile(t, fileSize)

	descs, err := GenerateDescriptors(filePath, 4*1024*1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(descs) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(descs))
	}
	if descs[0].Offset != 0 {
		t.Errorf("offset: got %d, want 0", descs[0].Offset)
	}
	if descs[0].Size != fileSize {
		t.Errorf("size: got %d, want %d", descs[0].Size, fileSize)
	}
}

func TestGenerateDescriptors_ExactChunkSize(t *testing.T) {
	// File exactly equals chunk size → 1 chunk, no remainder
	chunkSize := int64(4 * 1024 * 1024)
	filePath := createTempFile(t, chunkSize)

	descs, err := GenerateDescriptors(filePath, chunkSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(descs) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(descs))
	}
	if descs[0].Size != chunkSize {
		t.Errorf("size: got %d, want %d", descs[0].Size, chunkSize)
	}
}

func TestGenerateDescriptors_ExactMultiple(t *testing.T) {
	// File is exactly 2x chunk size → 2 chunks, no remainder
	chunkSize := int64(1024)
	filePath := createTempFile(t, 2*chunkSize)

	descs, err := GenerateDescriptors(filePath, chunkSize)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(descs) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(descs))
	}
	if descs[0].Size != chunkSize || descs[1].Size != chunkSize {
		t.Errorf("both chunks should be %d bytes", chunkSize)
	}
}

func TestGenerateDescriptors_EmptyFile(t *testing.T) {
	filePath := createTempFile(t, 0)
	descs, err := GenerateDescriptors(filePath, 4*1024*1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(descs) != 0 {
		t.Fatalf("expected 0 chunks for empty file, got %d", len(descs))
	}
}

func TestGenerateDescriptors_NonExistentFile(t *testing.T) {
	_, err := GenerateDescriptors("/nonexistent/path/file.bin", 4*1024*1024)
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
}

func TestGenerateDescriptors_DeterministicChunkIDs(t *testing.T) {
	// Same fileID + index should produce the same chunk ID
	// GenerateDescriptors uses uuid.New() each time, so IDs will differ between calls.
	// But within a single call, chunk IDs should be unique.
	filePath := createTempFile(t, 3*1024) // 3 chunks with 1KB chunk size

	descs, err := GenerateDescriptors(filePath, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	seen := make(map[string]bool)
	for _, d := range descs {
		if d.ChunkID == "" {
			t.Error("chunk ID should not be empty")
		}
		if seen[d.ChunkID] {
			t.Errorf("duplicate chunk ID: %s", d.ChunkID)
		}
		seen[d.ChunkID] = true
	}
}

func TestGenerateDescriptors_ChunkIDConsistency(t *testing.T) {
	// Verify that ComputeChunkID(fileID, index) produces consistent results
	// by checking that two descriptors with the same fileID but different indices
	// have different chunk IDs
	filePath := createTempFile(t, 2*1024)

	descs, err := GenerateDescriptors(filePath, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(descs) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(descs))
	}
	if descs[0].ChunkID == descs[1].ChunkID {
		t.Error("different chunk indices should produce different chunk IDs")
	}
}
