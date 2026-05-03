//go:build integration

// Package integration contains end-to-end integration tests that exercise
// the dfsclient.Client SDK against a fully running multi-node DFS cluster
// (3 metadata + 3 storage nodes). These tests validate the complete data
// pipeline: client SDK → metadata (Raft) → storage (gRPC streaming) and
// back, including checksum integrity verification.
//
// These tests complement the raw gRPC integration tests by exercising the
// same cluster topology through the public Client SDK, which is the same
// API the dfs-cli binary uses.
//
// # Prerequisites
//
//	None beyond a working Go toolchain. All cluster nodes run in-process.
//
// # Running
//
//	make integration-test
//	# or:
//	go test -tags integration -count=1 -timeout 120s -v ./integration/ -run TestClientE2E
//
// # Cluster Topology
//
//	3 metadata nodes forming a Raft consensus group (one leader, two followers)
//	3 storage nodes registered with the metadata cluster
//	All nodes use random ports; no external services required.
//
// # Test Coverage
//
//	- Upload single-chunk file (< chunk size)
//	- Upload multi-chunk file (> chunk size)
//	- Upload exact-chunk-boundary file
//	- Upload empty file (should fail)
//	- Upload via io.Reader (UploadReader)
//	- Download and verify byte-for-byte content
//	- Checksum integrity verification (client computes SHA-256, storage validates, download re-checks)
//	- List files (all, prefix-filtered)
//	- Delete file
//	- Upload then download round-trip
//	- Multiple file upload, list, download each
package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/satyam709/distributed-fs/client/dfsclient"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/internal/retry"
	"github.com/satyam709/distributed-fs/storage"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/store"
)

// ---------------------------------------------------------------------------
// FullCluster — multi-metadata + multi-storage cluster for client E2E tests.
// ---------------------------------------------------------------------------

// fullCluster wraps a multi-metadata Raft cluster plus multiple storage nodes
// suitable for exercising the dfsclient.Client SDK end-to-end.
type fullCluster struct {
	MetaCluster *MultiMetaCluster

	storageNodes  []*storage.StorageNode
	storageAddrs  []string
	cleanups      []func()
}

// startFullCluster boots nMeta metadata nodes (Raft cluster) and nStorage
// storage nodes, all in-process. The caller MUST call shutdown().
func startFullCluster(t *testing.T, nMeta, nStorage int) *fullCluster {
	t.Helper()

	fc := &fullCluster{}

	fc.MetaCluster = StartMultiMetadataCluster(t, nMeta)

	metaAddrs := make([]string, len(fc.MetaCluster.Addrs))
	copy(metaAddrs, fc.MetaCluster.Addrs)

	logger := logging.NewCLogger()
	for i := 0; i < nStorage; i++ {
		dir := t.TempDir()

		cs, err := store.NewChecksumIndexBoltDB[[]byte](store.ByteCodec{},
			store.WithDbPath[[]byte](dir),
		)
		if err != nil {
			t.Fatalf("checksum store %d: %v", i, err)
		}
		if err := cs.Open(); err != nil {
			t.Fatalf("checksum open %d: %v", i, err)
		}
		fc.cleanups = append(fc.cleanups, cs.CleanUp)

		ds, err := store.NewDiskStore(
			store.WithChecksumStore(cs),
			store.WithRootDir(dir),
			store.WithTempDir(dir),
			store.WithSplitLevel(2),
			store.WithTotalSpace(256*1024*1024),
		)
		if err != nil {
			t.Fatalf("disk store %d: %v", i, err)
		}

		rp := retry.Policy{MaxAttempts: 3, Base: 100 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2.0}
		mc, err := metaclient.NewMetadataClient(metaAddrs, rp, 10*time.Second)
		if err != nil {
			t.Fatalf("metaclient %d: %v", i, err)
		}

		nodeID := fmt.Sprintf("storage-%d", i)
		cfg := storage.StorageNodeConfig{
			NodeID:            nodeID,
			GRPCAddr:          "127.0.0.1:0",
			MetadataAddrs:     metaAddrs,
			DataDir:           dir,
			Timeout:           30 * time.Second,
			HeartbeatInterval: 1 * time.Second,
			ReplicationFactor: nStorage,
			RPCTimeout:        10 * time.Second,
			RetryMaxAttempts:  3,
			RetryBaseBackoff:  100 * time.Millisecond,
			RetryMaxBackoff:   5 * time.Second,
		}

		node, err := storage.NewStorageNode(cfg, logger, ds, mc)
		if err != nil {
			t.Fatalf("NewStorageNode %d: %v", i, err)
		}
		if err := node.Start(); err != nil {
			t.Fatalf("StorageNode.Start %d: %v", i, err)
		}

		addr := node.BoundAddr()
		t.Logf("e2e storage node %q on %s", nodeID, addr)

		fc.storageNodes = append(fc.storageNodes, node)
		fc.storageAddrs = append(fc.storageAddrs, addr)
	}

	time.Sleep(500 * time.Millisecond)

	return fc
}

func (fc *fullCluster) shutdown() {
	for _, n := range fc.storageNodes {
		n.Stop()
	}
	fc.MetaCluster.Shutdown()
	for i := len(fc.cleanups) - 1; i >= 0; i-- {
		fc.cleanups[i]()
	}
}

func (fc *fullCluster) metaAddrs() []string {
	return fc.MetaCluster.Addrs
}

// ---------------------------------------------------------------------------
// Client factory helper.
// ---------------------------------------------------------------------------

// newE2EClient creates a dfsclient.Client connected to the full cluster
// with reasonable defaults for integration testing.
func newE2EClient(t *testing.T, metaAddrs []string, tmpDir string) *dfsclient.Client {
	t.Helper()

	client, err := dfsclient.New(
		dfsclient.WithMetadataAddrs(metaAddrs...),
		dfsclient.WithChunkSize(64*1024),       // 64KB chunks
		dfsclient.WithMaxParallelUploads(3),
		dfsclient.WithMaxParallelDownloads(3),
		dfsclient.WithManifestDir(filepath.Join(tmpDir, "manifests")),
		dfsclient.WithOutputDir(filepath.Join(tmpDir, "downloads")),
		dfsclient.WithRetryAttempts(3),
		dfsclient.WithFrameSize(32*1024),
	)
	if err != nil {
		t.Fatalf("dfsclient.New: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// createTestFile writes random data of the given size and returns the path
// and the original data for later verification.
func createTestFile(t *testing.T, dir string, size int64) (path string, data []byte) {
	t.Helper()

	data = make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	path = filepath.Join(dir, "e2e-test-file.bin")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	return path, data
}

// verifyDownload reads the downloaded file and compares with expected data.
func verifyDownload(t *testing.T, outputPath string, expected []byte) {
	t.Helper()
	downloaded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(downloaded, expected) {
		t.Errorf("downloaded data mismatch: got %d bytes, want %d bytes", len(downloaded), len(expected))
	}
}

// =======================================================================
// E2E TESTS
// =======================================================================

// TestClientE2E_SmallFileUploadDownload uploads a file smaller than one chunk
// and verifies it can be downloaded byte-for-byte.
func TestClientE2E_SmallFileUploadDownload(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	filePath, originalData := createTestFile(t, tmpDir, 42) // 42 bytes, well under 64KB chunk
	remoteName := "small-file.bin"

	uploadResult, err := client.Upload(context.Background(), filePath, remoteName, nil)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	t.Logf("Uploaded: %+v", uploadResult)
	if uploadResult.ChunksDone != 1 {
		t.Errorf("expected 1 chunk, got %d", uploadResult.ChunksDone)
	}

	outputPath := filepath.Join(tmpDir, "small-downloaded.bin")
	downloadResult, err := client.Download(context.Background(), remoteName, outputPath, nil)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	t.Logf("Downloaded: %+v", downloadResult)

	verifyDownload(t, outputPath, originalData)
}

// TestClientE2E_MultiChunkUploadDownload uploads a file spanning multiple
// chunks (larger than chunk size) and verifies the full round-trip.
func TestClientE2E_MultiChunkUploadDownload(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	// 200KB → 4 chunks of 64KB (last chunk partial)
	filePath, originalData := createTestFile(t, tmpDir, 200*1024)
	remoteName := "large-file.bin"

	uploadResult, err := client.Upload(context.Background(), filePath, remoteName, nil)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if uploadResult.ChunksDone != 4 {
		t.Errorf("expected 4 chunks, got %d", uploadResult.ChunksDone)
	}

	outputPath := filepath.Join(tmpDir, "large-downloaded.bin")
	_, err = client.Download(context.Background(), remoteName, outputPath, nil)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	verifyDownload(t, outputPath, originalData)
}

// TestClientE2E_ExactChunkBoundaryFile uploads a file whose size exactly
// equals one chunk (64KB) to test edge-case handling.
func TestClientE2E_ExactChunkBoundaryFile(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	filePath, originalData := createTestFile(t, tmpDir, 64*1024) // exactly 64KB
	remoteName := "boundary-file.bin"

	uploadResult, err := client.Upload(context.Background(), filePath, remoteName, nil)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if uploadResult.ChunksDone != 1 {
		t.Errorf("expected 1 chunk, got %d", uploadResult.ChunksDone)
	}

	outputPath := filepath.Join(tmpDir, "boundary-downloaded.bin")
	_, err = client.Download(context.Background(), remoteName, outputPath, nil)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	verifyDownload(t, outputPath, originalData)
}

// TestClientE2E_EmptyFileRejected verifies that uploading an empty file
// returns an error (as per the upload service's contract).
func TestClientE2E_EmptyFileRejected(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	filePath, _ := createTestFile(t, tmpDir, 0)
	remoteName := "empty.bin"

	_, err := client.Upload(context.Background(), filePath, remoteName, nil)
	if err == nil {
		t.Fatal("expected error for empty file, got nil")
	}
	t.Logf("Correctly rejected empty file: %v", err)
}

// TestClientE2E_UploadReader uploads data from an io.Reader (bytes.Buffer)
// instead of a file path, verifying the streaming upload path works.
func TestClientE2E_UploadReader(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	originalData := make([]byte, 128*1024) // 128KB
	for i := range originalData {
		originalData[i] = byte((i * 7) % 251)
	}
	reader := bytes.NewReader(originalData)

	uploadResult, err := client.UploadReader(context.Background(), reader, int64(len(originalData)), "reader-file.bin", nil)
	if err != nil {
		t.Fatalf("UploadReader: %v", err)
	}
	if uploadResult.ChunksDone != 2 {
		t.Errorf("expected 2 chunks (128KB/64KB), got %d", uploadResult.ChunksDone)
	}

	outputPath := filepath.Join(tmpDir, "reader-downloaded.bin")
	_, err = client.Download(context.Background(), "reader-file.bin", outputPath, nil)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	verifyDownload(t, outputPath, originalData)
}

// TestClientE2E_ListFiles uploads multiple files and verifies that List
// returns all of them with correct metadata, including prefix filtering.
func TestClientE2E_ListFiles(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	fileNames := []string{"report-q1.pdf", "report-q2.pdf", "data-log.csv", "notes.txt"}
	for _, name := range fileNames {
		filePath, _ := createTestFile(t, tmpDir, 512)
		_, err := client.Upload(context.Background(), filePath, name, nil)
		if err != nil {
			t.Fatalf("Upload %s: %v", name, err)
		}
	}

	files, err := client.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(files) != len(fileNames) {
		t.Errorf("List all: got %d files, want %d", len(files), len(fileNames))
	}

	// Verify file names are present
	namesSeen := make(map[string]bool)
	for _, f := range files {
		namesSeen[f.FileName] = true
	}
	for _, name := range fileNames {
		if !namesSeen[name] {
			t.Errorf("file %q not found in listing", name)
		}
	}

	// Prefix filter: "report" should return 2 files
	reports, err := client.List(context.Background(), "report")
	if err != nil {
		t.Fatalf("List prefix: %v", err)
	}
	if len(reports) != 2 {
		t.Errorf("List prefix 'report': got %d files, want 2", len(reports))
	}
	for _, f := range reports {
		if !strings.HasPrefix(f.FileName, "report") {
			t.Errorf("prefix filter: %q does not start with 'report'", f.FileName)
		}
	}

	// Prefix filter: "nonexistent" should return 0
	none, err := client.List(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("List nonexistent: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("List 'nonexistent': got %d files, want 0", len(none))
	}
}

// TestClientE2E_DeleteFile uploads a file, deletes it, and verifies it's gone
// from listings.
func TestClientE2E_DeleteFile(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	filePath, _ := createTestFile(t, tmpDir, 32*1024) // 32KB
	remoteName := "to-delete.bin"

	uploadResult, err := client.Upload(context.Background(), filePath, remoteName, nil)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Verify file exists in listing
	files, err := client.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List before delete: %v", err)
	}
	found := false
	for _, f := range files {
		if f.FileName == remoteName {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("file not found in listing before delete")
	}

	// Delete the file
	if err := client.Delete(context.Background(), uploadResult.FileID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify file status is now marked as deleted
	files, err = client.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List after delete: %v", err)
	}
	for _, f := range files {
		if f.FileName == remoteName {
			if f.Status != "deleted" {
				t.Errorf("file status after delete: got %q, want %q", f.Status, "deleted")
			}
			break
		}
	}

	// The system uses soft-deletes; files remain in metadata with "deleted" status.
	// Content may still be retrievable from storage nodes.
}

// TestClientE2E_ChecksumIntegrity verifies that checksums computed by the
// client SDK match what the storage nodes compute, protecting against
// corruption during the upload pipeline. This was the bug that motivated
// switching checksums from hex-encoded strings to raw [32]byte.
func TestClientE2E_ChecksumIntegrity(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	// Use deterministic data so checksums are reproducible
	filePath, originalData := createTestFile(t, tmpDir, 100*1024) // 100KB
	remoteName := "checksum-integrity.bin"

	_, err := client.Upload(context.Background(), filePath, remoteName, nil)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Download and verify the data is intact
	outputPath := filepath.Join(tmpDir, "checksum-download.bin")
	_, err = client.Download(context.Background(), remoteName, outputPath, nil)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	verifyDownload(t, outputPath, originalData)
}

// TestClientE2E_MultipleFiles uploads 3 files of different sizes, lists them,
// downloads each, and verifies all content matches.
func TestClientE2E_MultipleFiles(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	type testFile struct {
		name string
		size int64
		data []byte
	}

	files := []testFile{
		{name: "file-a.bin", size: 10 * 1024},   // 10KB
		{name: "file-b.bin", size: 100 * 1024},  // 100KB, multi-chunk
		{name: "file-c.bin", size: 50 * 1024},   // 50KB
	}

	for i := range files {
		path, data := createTestFile(t, tmpDir, files[i].size)
		files[i].data = data
		_, err := client.Upload(context.Background(), path, files[i].name, nil)
		if err != nil {
			t.Fatalf("Upload %s: %v", files[i].name, err)
		}
	}

	// List and verify all files are present
	listing, err := client.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listing) != len(files) {
		t.Errorf("List: got %d files, want %d", len(listing), len(files))
	}

	// Download each file and verify
	for _, tf := range files {
		outputPath := filepath.Join(tmpDir, "dl-"+tf.name)
		_, err := client.Download(context.Background(), tf.name, outputPath, nil)
		if err != nil {
			t.Errorf("Download %s: %v", tf.name, err)
			continue
		}
		verifyDownload(t, outputPath, tf.data)
	}
}

// TestClientE2E_ProgressCallback verifies that progress callbacks fire
// during upload and download operations.
func TestClientE2E_ProgressCallback(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	filePath, _ := createTestFile(t, tmpDir, 200*1024)
	remoteName := "progress-test.bin"

	var uploadCalls int
	uploadResult, err := client.Upload(context.Background(), filePath, remoteName, func(info dfsclient.ProgressInfo) {
		uploadCalls++
		t.Logf("Upload progress: chunk %d/%d (%d/%d bytes)",
			info.ChunkIndex, info.ChunksTotal, info.BytesDone, info.BytesTotal)
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if uploadCalls == 0 {
		t.Error("upload progress callback was never called")
	}
	if uploadCalls != uploadResult.ChunksTotal {
		t.Errorf("upload progress calls: got %d, want %d", uploadCalls, uploadResult.ChunksTotal)
	}

	var downloadCalls int
	outputPath := filepath.Join(tmpDir, "progress-downloaded.bin")
	_, err = client.Download(context.Background(), remoteName, outputPath, func(info dfsclient.ProgressInfo) {
		downloadCalls++
		t.Logf("Download progress: chunk %d/%d", info.ChunkIndex, info.ChunksTotal)
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if downloadCalls == 0 {
		t.Error("download progress callback was never called")
	}
	if downloadCalls != uploadResult.ChunksTotal {
		t.Errorf("download progress calls: got %d, want %d", downloadCalls, uploadResult.ChunksTotal)
	}
}

// TestClientE2E_NonExistentDownload verifies that downloading a file that
// was never uploaded returns a proper error.
func TestClientE2E_NonExistentDownload(t *testing.T) {
	fc := startFullCluster(t, 3, 3)
	defer fc.shutdown()

	tmpDir := t.TempDir()
	client := newE2EClient(t, fc.metaAddrs(), tmpDir)

	outputPath := filepath.Join(tmpDir, "ghost.bin")
	_, err := client.Download(context.Background(), "nonexistent-file.bin", outputPath, nil)
	if err == nil {
		t.Fatal("expected error for non-existent file, got nil")
	}
	t.Logf("Correctly rejected non-existent file: %v", err)
}
