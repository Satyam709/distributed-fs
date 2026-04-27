package manifest

import (
	"fmt"
	"sync"
	"testing"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	return NewManager(t.TempDir())
}

func sampleManifest() *UploadManifest {
	return &UploadManifest{
		FileID:    "test-file-id-001",
		Filename:  "report.pdf",
		TotalSize: 10 * 1024 * 1024,
		ChunkSize: 4 * 1024 * 1024,
		ChunkStatus: map[string]bool{
			"chunk-aaa": false,
			"chunk-bbb": false,
			"chunk-ccc": false,
		},
	}
}

func TestCreateAndLoad(t *testing.T) {
	mgr := newTestManager(t)
	m := sampleManifest()

	if err := mgr.Create(m); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	loaded, err := mgr.Load(m.FileID)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if loaded.FileID != m.FileID {
		t.Errorf("FileID: got %s, want %s", loaded.FileID, m.FileID)
	}
	if loaded.Filename != m.Filename {
		t.Errorf("Filename: got %s, want %s", loaded.Filename, m.Filename)
	}
	if loaded.TotalSize != m.TotalSize {
		t.Errorf("TotalSize: got %d, want %d", loaded.TotalSize, m.TotalSize)
	}
	if loaded.ChunkSize != m.ChunkSize {
		t.Errorf("ChunkSize: got %d, want %d", loaded.ChunkSize, m.ChunkSize)
	}
	if len(loaded.ChunkStatus) != len(m.ChunkStatus) {
		t.Errorf("ChunkStatus length: got %d, want %d", len(loaded.ChunkStatus), len(m.ChunkStatus))
	}
	for id, done := range loaded.ChunkStatus {
		if done {
			t.Errorf("chunk %s should be pending, got done", id)
		}
	}
}

func TestExists(t *testing.T) {
	mgr := newTestManager(t)
	m := sampleManifest()

	if mgr.Exists(m.FileID) {
		t.Error("Exists should return false before Create")
	}

	if err := mgr.Create(m); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if !mgr.Exists(m.FileID) {
		t.Error("Exists should return true after Create")
	}
}

func TestMarkChunkDone(t *testing.T) {
	mgr := newTestManager(t)
	m := sampleManifest()

	if err := mgr.Create(m); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := mgr.MarkChunkDone(m.FileID, "chunk-aaa"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}

	loaded, err := mgr.Load(m.FileID)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if !loaded.ChunkStatus["chunk-aaa"] {
		t.Error("chunk-aaa should be done")
	}
	if loaded.ChunkStatus["chunk-bbb"] {
		t.Error("chunk-bbb should still be pending")
	}
	if loaded.ChunkStatus["chunk-ccc"] {
		t.Error("chunk-ccc should still be pending")
	}
}

func TestDelete(t *testing.T) {
	mgr := newTestManager(t)
	m := sampleManifest()

	if err := mgr.Create(m); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !mgr.Exists(m.FileID) {
		t.Fatal("manifest should exist after Create")
	}

	if err := mgr.Delete(m.FileID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if mgr.Exists(m.FileID) {
		t.Error("manifest should not exist after Delete")
	}
}

func TestDeleteNonExistent(t *testing.T) {
	mgr := newTestManager(t)
	// Deleting a non-existent manifest should not error
	if err := mgr.Delete("does-not-exist"); err != nil {
		t.Errorf("Delete non-existent should not error, got: %v", err)
	}
}

func TestGetPendingChunks(t *testing.T) {
	mgr := newTestManager(t)
	m := sampleManifest()

	if err := mgr.Create(m); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// All chunks should be pending initially
	pending, err := mgr.GetPendingChunks(m.FileID)
	if err != nil {
		t.Fatalf("GetPendingChunks failed: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending chunks, got %d", len(pending))
	}

	// Mark one done
	if err := mgr.MarkChunkDone(m.FileID, "chunk-bbb"); err != nil {
		t.Fatalf("MarkChunkDone failed: %v", err)
	}

	pending, err = mgr.GetPendingChunks(m.FileID)
	if err != nil {
		t.Fatalf("GetPendingChunks failed: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending chunks, got %d", len(pending))
	}

	// Ensure the done chunk is not in the pending list
	for _, id := range pending {
		if id == "chunk-bbb" {
			t.Error("chunk-bbb should not be in pending list")
		}
	}
}

func TestGetPendingChunks_AllDone(t *testing.T) {
	mgr := newTestManager(t)
	m := sampleManifest()

	if err := mgr.Create(m); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	for id := range m.ChunkStatus {
		if err := mgr.MarkChunkDone(m.FileID, id); err != nil {
			t.Fatalf("MarkChunkDone(%s) failed: %v", id, err)
		}
	}

	pending, err := mgr.GetPendingChunks(m.FileID)
	if err != nil {
		t.Fatalf("GetPendingChunks failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending chunks, got %d", len(pending))
	}
}

func TestConcurrentMarkChunkDone(t *testing.T) {
	mgr := newTestManager(t)

	// Create manifest with many chunks
	chunks := make(map[string]bool)
	for i := 0; i < 20; i++ {
		chunks[fmt.Sprintf("chunk-%03d", i)] = false
	}
	m := &UploadManifest{
		FileID:      "concurrent-test",
		Filename:    "big.dat",
		TotalSize:   20 * 1024,
		ChunkSize:   1024,
		ChunkStatus: chunks,
	}

	if err := mgr.Create(m); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Mark all chunks done concurrently
	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	for id := range chunks {
		wg.Add(1)
		go func(chunkID string) {
			defer wg.Done()
			if err := mgr.MarkChunkDone(m.FileID, chunkID); err != nil {
				errCh <- err
			}
		}(id)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent MarkChunkDone error: %v", err)
	}

	// Verify all done
	pending, err := mgr.GetPendingChunks(m.FileID)
	if err != nil {
		t.Fatalf("GetPendingChunks failed: %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after concurrent marks, got %d", len(pending))
	}
}

func TestLoadNonExistent(t *testing.T) {
	mgr := newTestManager(t)
	_, err := mgr.Load("nonexistent-file-id")
	if err == nil {
		t.Error("Load non-existent should return error")
	}
}
