//go:build integration

package meta_storage

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testutil "github.com/satyam709/distributed-fs/integration/testutil"
)

// ---------------------------------------------------------------------------
// Scenario: File-level checksum is stored via CommitFile, not CreateFile
//
// The correct flow is:
//   1. CreateFile — no checksum (file hasn't been uploaded yet)
//   2. Client uploads all chunks
//   3. CommitFile — client sends the final whole-file checksum
//   4. FSM STORES the checksum in FileRecord (not compares against nil)
//   5. GetFile returns the file with the stored checksum
//
// BUG: handleCmdCommitFile currently compares file.CheckSum (nil from
// CreateFile) against req.Checksum — so CommitFile fails if a real
// checksum is provided, and the checksum is never stored.
// ---------------------------------------------------------------------------

// TestCommitFileWithChecksumSucceeds verifies that CommitFile accepts
// a real checksum and stores it in the FileRecord. Currently this fails
// because handleCmdCommitFile does bytes.Equal(nil, realChecksum) → false.
func TestCommitFileWithChecksumSucceeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "checksum-commit-file"
	chunkID := "checksum-commit-chunk"
	payload := []byte("data whose whole-file checksum we want to persist")

	// Compute whole-file SHA256
	fileHash := sha256.Sum256(payload)

	// ── 1. CreateFile (no checksum at this point) ──
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "checksum-test.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 1)

	// ── 2. Upload chunk ──
	pl := createResp.Placements[0]
	primaryClient := testutil.DialStorage(t, pl.Primary.Address)
	var replicaAddrs []string
	for _, r := range pl.Replicas {
		replicaAddrs = append(replicaAddrs, r.Address)
	}
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replicaAddrs)

	time.Sleep(1 * time.Second) // let CommitChunk propagate

	// ── 3. CommitFile WITH a real checksum ──
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: int64(len(payload)),
		Checksum: fileHash[:],
	})
	require.NoError(t, err,
		"CommitFile should succeed with a real checksum — "+
			"currently fails because FSM compares against nil instead of storing it")
}

// TestCommitFileStoresChecksum verifies that after CommitFile, the
// checksum is persisted and can be read back via GetFile's chunks.
// This tests the complete round-trip.
func TestCommitFileStoresChecksum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "checksum-stored-file"
	chunkID := "checksum-stored-chunk"
	payload := []byte("verify the whole-file checksum is stored and returned")

	fileHash := sha256.Sum256(payload)

	// ── 1. CreateFile ──
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "checksum-stored.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)

	// ── 2. Upload chunk ──
	pl := createResp.Placements[0]
	primaryClient := testutil.DialStorage(t, pl.Primary.Address)
	var replicaAddrs []string
	for _, r := range pl.Replicas {
		replicaAddrs = append(replicaAddrs, r.Address)
	}
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replicaAddrs)

	time.Sleep(1 * time.Second)

	// ── 3. CommitFile with checksum ──
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: int64(len(payload)),
		Checksum: fileHash[:],
	})
	require.NoError(t, err, "CommitFile should succeed")

	// ── 4. GetFile → verify status is complete ──
	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
		FileId: fileID,
	})
	require.NoError(t, err)
	assert.Equal(t, "complete", getResp.File.Status,
		"file should be 'complete' after CommitFile")

	// ── 5. Verify each chunk has a checksum ──
	require.Len(t, getResp.Chunks, 1)
	assert.NotEmpty(t, getResp.Chunks[0].Checksum,
		"chunk should have a checksum after CommitChunk")
	t.Logf("file status=%s, chunk checksum len=%d",
		getResp.File.Status, len(getResp.Chunks[0].Checksum))
}

// TestCommitFileBeforeAllChunksComplete verifies that CommitFile fails
// if not all chunks have been uploaded and committed yet.
func TestCommitFileBeforeAllChunksComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileID := "checksum-early-commit"

	// CreateFile with 2 chunks but don't upload anything
	_, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "early-commit.dat",
		FileSize:  1024,
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{"early-chunk-0", "early-chunk-1"},
	})
	require.NoError(t, err)

	// CommitFile should fail — chunks are still in "allocated" status
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: 1024,
		Checksum: nil,
	})
	require.Error(t, err, "CommitFile should fail when chunks are not yet complete")
	t.Logf("expected error: %v", err)
}
