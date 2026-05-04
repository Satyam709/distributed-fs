//go:build integration

package meta_storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testutil "github.com/satyam709/distributed-fs/integration/testutil"
)

// ---------------------------------------------------------------------------
// Scenario 2: Full Upload → Download round-trip
// ---------------------------------------------------------------------------

// TestUploadAndDownloadSingleChunk exercises the complete write path:
//
//	Client → CreateFile → PutChunk (to primary) → storage auto-commits →
//	CommitFile → GetFile → GetChunk → verify bytes match.
//
// This is the most critical integration test — it proves the entire data
// plane works end-to-end across real gRPC connections.
func TestUploadAndDownloadSingleChunk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "upload-dl-single-file"
	chunkID := "upload-dl-single-chunk"
	fileName := "hello.txt"
	payload := []byte("Hello, distributed world! This is integration test data.")

	// ── 1. CreateFile → get placement ──
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  fileName,
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024, // 4 MB
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err, "CreateFile should succeed")
	require.Len(t, createResp.Placements, 1)

	placement := createResp.Placements[0]
	primary := placement.Primary
	require.NotNil(t, primary)
	t.Logf("primary: %s @ %s, replicas: %d",
		primary.NodeId, primary.Address, len(placement.Replicas))

	// Build replica address list for PutChunk fan-out.
	var replicaAddrs []string
	for _, r := range placement.Replicas {
		replicaAddrs = append(replicaAddrs, r.Address)
	}

	// ── 2. PutChunk to primary (with replica fan-out) ──
	primaryClient := testutil.DialStorage(t, primary.Address)
	checksum := testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replicaAddrs)
	t.Logf("PutChunk succeeded, checksum=%x", checksum)

	// Give the storage node a moment to async-commit to metadata.
	time.Sleep(1 * time.Second)

	// ── 3. CommitFile with whole-file checksum ──
	fileHash := sha256.Sum256(payload)
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: int64(len(payload)),
		Checksum: fileHash[:],
	})
	require.NoError(t, err, "CommitFile should succeed")

	// ── 4. GetFile → verify metadata ──
	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
		FileId: fileID,
	})
	require.NoError(t, err, "GetFile should succeed")
	assert.Equal(t, fileID, getResp.File.FileId)
	assert.Equal(t, fileName, getResp.File.FileName)
	assert.Equal(t, "complete", getResp.File.Status,
		"file status should be 'complete' after CommitFile")
	require.Len(t, getResp.Chunks, 1)
	assert.Equal(t, chunkID, getResp.Chunks[0].ChunkId)
	t.Logf("file metadata: status=%s, chunks=%d, replicas=%v",
		getResp.File.Status, len(getResp.Chunks), getResp.Chunks[0].Replicas)

	// ── 5. GetChunk → verify data round-trip ──
	downloaded := testutil.GetChunkData(t, ctx, primaryClient, chunkID)
	assert.Equal(t, payload, downloaded,
		"downloaded data must match uploaded data byte-for-byte")
	t.Logf("data round-trip verified: %d bytes", len(downloaded))
}

// TestUploadWithReplication verifies that chunk data is replicated to
// secondary nodes. After uploading through the primary, the chunk should
// also be readable from the replica.
func TestUploadWithReplication(t *testing.T) {
	if len(tc.StorageNodes) < 2 {
		t.Skip("replication test requires at least 2 storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "repl-test-file"
	chunkID := "repl-test-chunk"
	payload := []byte("Replicated data for integration test — must appear on both nodes.")

	// ── 1. CreateFile ──
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "replicated.bin",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024, // 4 MB
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 1)

	placement := createResp.Placements[0]
	primary := placement.Primary
	require.NotNil(t, primary)

	var replicaAddrs []string
	for _, r := range placement.Replicas {
		replicaAddrs = append(replicaAddrs, r.Address)
	}
	require.NotEmpty(t, replicaAddrs, "expected at least 1 replica")

	// ── 2. PutChunk with replication ──
	primaryClient := testutil.DialStorage(t, primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replicaAddrs)

	// Wait for replication to complete.
	time.Sleep(2 * time.Second)

	// ── 3. Verify chunk is readable from the replica ──
	replicaClient := testutil.DialStorage(t, replicaAddrs[0])
	replicaData := testutil.GetChunkData(t, ctx, replicaClient, chunkID)
	assert.Equal(t, payload, replicaData,
		"replica should serve identical data to what was uploaded")
	t.Logf("replication verified: primary=%s, replica=%s, %d bytes",
		primary.Address, replicaAddrs[0], len(replicaData))
}

// TestUploadMultiChunkFile uploads a file with multiple chunks and verifies
// all chunks are stored and the file metadata tracks them correctly.
func TestUploadMultiChunkFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "multi-chunk-file"
	chunkIDs := []string{"mc-chunk-0", "mc-chunk-1", "mc-chunk-2"}
	payloads := [][]byte{
		[]byte("Chunk zero — first part of the file."),
		[]byte("Chunk one — middle section of the file."),
		[]byte("Chunk two — the final part of the file."),
	}

	var totalSize int64
	for _, p := range payloads {
		totalSize += int64(len(p))
	}

	// ── 1. CreateFile with 3 chunks ──
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "multi.dat",
		FileSize:  totalSize,
		ChunkSize: 4 * 1024 * 1024, // 4 MB
		ChunkIds:  chunkIDs,
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 3, "expected 3 chunk placements")

	// ── 2. Upload each chunk to its assigned primary ──
	for i, pl := range createResp.Placements {
		primaryClient := testutil.DialStorage(t, pl.Primary.Address)
		var replicaAddrs []string
		for _, r := range pl.Replicas {
			replicaAddrs = append(replicaAddrs, r.Address)
		}
		testutil.PutChunkData(t, ctx, primaryClient, chunkIDs[i], fileID, payloads[i], replicaAddrs)
		t.Logf("uploaded chunk %s → %s", chunkIDs[i], pl.Primary.Address)
	}

	time.Sleep(1 * time.Second) // let commits propagate

	// ── 3. CommitFile with whole-file checksum ──
	var allPayloads []byte
	for _, p := range payloads {
		allPayloads = append(allPayloads, p...)
	}
	fileHash := sha256.Sum256(allPayloads)
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: totalSize,
		Checksum: fileHash[:],
	})
	require.NoError(t, err)

	// ── 4. GetFile → verify all chunks tracked ──
	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})
	require.NoError(t, err)
	assert.Equal(t, "complete", getResp.File.Status)
	require.Len(t, getResp.Chunks, 3)
	for i, ck := range getResp.Chunks {
		assert.Equal(t, chunkIDs[i], ck.ChunkId,
			fmt.Sprintf("chunk %d ID mismatch", i))
	}
	t.Logf("multi-chunk file verified: %d chunks, status=%s", len(getResp.Chunks), getResp.File.Status)

	// ── 5. Download each chunk and verify data ──
	for i, pl := range createResp.Placements {
		client := testutil.DialStorage(t, pl.Primary.Address)
		got := testutil.GetChunkData(t, ctx, client, chunkIDs[i])
		assert.Equal(t, payloads[i], got, "chunk %d data mismatch", i)
	}
}

// TestFileStatusWithoutCommitFile verifies that after uploading chunks but
// NOT calling CommitFile, the file status remains "creating".
//
// Bug being exposed: the DFS client upload path never calls CommitFile,
// so files stay in "creating" status forever.
func TestFileStatusWithoutCommitFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "no-commit-file"
	chunkID := "no-commit-chunk"
	payload := []byte("File without CommitFile call — should remain 'creating'.")

	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "no-commit.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)

	placement := createResp.Placements[0]
	primaryClient := testutil.DialStorage(t, placement.Primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, nil)

	time.Sleep(1 * time.Second)

	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})

	if err == nil {
		t.Logf("file status (no CommitFile): %s", getResp.File.Status)
	}

	require.Error(t, err, "GetFile should reject files in 'creating' status")
	assert.Contains(t, err.Error(), "not ready",
		"error should indicate file is not ready")
	t.Logf("GetFile correctly rejected non-committed file: %v", err)
}

// TestFileStatusWithCommitFile verifies that after uploading chunks AND
// calling CommitFile, the file status transitions to "complete".
//
// This is the expected happy path that Bug 1 should achieve automatically.
func TestFileStatusWithCommitFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "with-commit-file"
	chunkID := "with-commit-chunk"
	payload := []byte("File with CommitFile call — should become 'complete'.")

	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "with-commit.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)

	placement := createResp.Placements[0]
	primaryClient := testutil.DialStorage(t, placement.Primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, nil)

	time.Sleep(1 * time.Second)

	fileHash := sha256.Sum256(payload)
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: int64(len(payload)),
		Checksum: fileHash[:],
	})
	require.NoError(t, err)

	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})
	require.NoError(t, err)

	t.Logf("file status (with CommitFile): %s", getResp.File.Status)
	assert.Equal(t, "complete", getResp.File.Status,
		"file should transition to 'complete' after CommitFile")

	t.Logf("chunks in file: %d, replicas: %v",
		len(getResp.Chunks), getResp.Chunks[0].Replicas)
}

// TestFullUploadDownloadRoundTrip exercises the complete upload→commit→download
// flow for a multi-chunk file with replication, then verifies every chunk is
// readable from every replica.
//
// This is the golden test for both Bug 1 (file commit) and Bug 2 (replica addresses).
func TestFullUploadDownloadRoundTrip(t *testing.T) {
	if len(tc.StorageNodes) < 3 {
		t.Skip("needs 3+ storage nodes")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fileID := "golden-roundtrip"
	chunkIDs := []string{"golden-ck-0", "golden-ck-1", "golden-ck-2"}
	payloads := [][]byte{
		[]byte("Golden chunk zero data."),
		[]byte("Golden chunk one data."),
		[]byte("Golden chunk two data."),
	}
	var totalSize int64
	for _, p := range payloads {
		totalSize += int64(len(p))
	}

	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "golden.dat",
		FileSize:  totalSize,
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  chunkIDs,
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 3)

	for i, pl := range createResp.Placements {
		primaryClient := testutil.DialStorage(t, pl.Primary.Address)
		var replAddrs []string
		for _, r := range pl.Replicas {
			replAddrs = append(replAddrs, r.Address)
		}
		testutil.PutChunkData(t, ctx, primaryClient, chunkIDs[i], fileID, payloads[i], replAddrs)
	}

	time.Sleep(2 * time.Second)

	var allData []byte
	for _, p := range payloads {
		allData = append(allData, p...)
	}
	fileHash := sha256.Sum256(allData)
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: totalSize,
		Checksum: fileHash[:],
	})
	require.NoError(t, err)

	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})
	require.NoError(t, err)
	assert.Equal(t, "complete", getResp.File.Status,
		"file should be 'complete' after full upload+commit")
	assert.Equal(t, fileID, getResp.File.FileId)
	assert.Equal(t, "golden.dat", getResp.File.FileName)
	require.Len(t, getResp.Chunks, 3)

	for i, ck := range getResp.Chunks {
		assert.Equal(t, chunkIDs[i], ck.ChunkId)
		assert.NotEmpty(t, ck.Replicas, "chunk %s should have replicas", ck.ChunkId)
		for _, rep := range ck.Replicas {
			assert.Contains(t, rep, ":",
				"replica address %q for chunk %s should contain port", rep, ck.ChunkId)
		}
		t.Logf("chunk %s: %d replicas → %v", ck.ChunkId, len(ck.Replicas), ck.Replicas)
	}

	for i, pl := range createResp.Placements {
		primaryClient := testutil.DialStorage(t, pl.Primary.Address)
		got := testutil.GetChunkData(t, ctx, primaryClient, chunkIDs[i])
		assert.Equal(t, payloads[i], got, "chunk %d data mismatch on primary", i)

		for _, r := range pl.Replicas {
			replClient := testutil.DialStorage(t, r.Address)
			replGot := testutil.GetChunkData(t, ctx, replClient, chunkIDs[i])
			assert.Equal(t, payloads[i], replGot,
				"chunk %d data mismatch on replica %s", i, r.Address)
		}
	}

	t.Logf("golden round-trip verified: %d chunks, all replicas readable", len(chunkIDs))
}
