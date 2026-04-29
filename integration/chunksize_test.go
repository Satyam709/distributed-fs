//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Scenario: ChunkSize persistence through metadata FSM
//
// These tests verify that the chunk_size declared by the client at CreateFile
// is persisted in the metadata FSM and returned on GetFile. This is critical
// because download uses chunk_size from metadata to compute write offsets.
// ---------------------------------------------------------------------------

// TestChunkSizePersisted verifies that the chunk_size sent in CreateFile
// is persisted in the FSM and returned by GetFile.
//
// BUG: Currently fails because CommandCreateFile doesn't have a ChunkSize
// field, so FileRecord.ChunkSize is always 0.
func TestChunkSizePersisted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileID := "chunksize-persist-file"
	chunkID := "chunksize-persist-chunk"
	var chunkSize int64 = 4 * 1024 * 1024 // 4 MB

	_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "chunksize-test.dat",
		FileSize:  10 * 1024 * 1024, // 10 MB file
		ChunkSize: chunkSize,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err, "CreateFile should succeed")

	// Retrieve the file and check chunk_size.
	getResp, err := testCluster.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
		FileId: fileID,
	})
	require.NoError(t, err, "GetFile should succeed")

	assert.Equal(t, chunkSize, getResp.File.ChunkSize,
		"ChunkSize returned by GetFile must match the value sent in CreateFile")
	t.Logf("ChunkSize: sent=%d, got=%d", chunkSize, getResp.File.ChunkSize)
}

// TestChunkSizePersistedDifferentValues verifies that two files uploaded
// with different chunk sizes each retain their own chunk_size. This proves
// the system supports per-file chunk sizes, not a global constant.
func TestChunkSizePersistedDifferentValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		fileID    string
		chunkSize int64
	}{
		{"cs-diff-file-1mb", 1 * 1024 * 1024},  // 1 MB
		{"cs-diff-file-8mb", 8 * 1024 * 1024},  // 8 MB
		{"cs-diff-file-16mb", 16 * 1024 * 1024}, // 16 MB
	}

	for _, tc := range tests {
		_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
			FileId:    tc.fileID,
			FileName:  tc.fileID + ".dat",
			FileSize:  100 * 1024 * 1024, // 100 MB
			ChunkSize: tc.chunkSize,
			ChunkIds:  []string{tc.fileID + "-chunk-0"},
		})
		require.NoError(t, err, "CreateFile(%s) should succeed", tc.fileID)
	}

	for _, tc := range tests {
		getResp, err := testCluster.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
			FileId: tc.fileID,
		})
		require.NoError(t, err, "GetFile(%s) should succeed", tc.fileID)
		assert.Equal(t, tc.chunkSize, getResp.File.ChunkSize,
			"file %s: ChunkSize mismatch", tc.fileID)
		t.Logf("file %s: expected=%d, got=%d ✓", tc.fileID, tc.chunkSize, getResp.File.ChunkSize)
	}
}

// TestChunkSizeInListFiles verifies that ChunkSize is also returned
// correctly in ListFiles responses.
func TestChunkSizeInListFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileID := "cs-list-file"
	var chunkSize int64 = 2 * 1024 * 1024 // 2 MB

	_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "cs-list-test.dat",
		FileSize:  20 * 1024 * 1024,
		ChunkSize: chunkSize,
		ChunkIds:  []string{fileID + "-chunk-0"},
	})
	require.NoError(t, err)

	listResp, err := testCluster.MetaC.ListFiles(ctx, &pb_meta.ListFilesRequest{})
	require.NoError(t, err)

	var found bool
	for _, f := range listResp.Files {
		if f.FileId == fileID {
			found = true
			assert.Equal(t, chunkSize, f.ChunkSize,
				"ChunkSize in ListFiles response should match CreateFile value")
			break
		}
	}
	assert.True(t, found, "file %s should appear in ListFiles", fileID)
}

// TestMultiChunkDownloadOffsetCorrectness is the critical test that proves
// the download data-corruption bug. It uploads a multi-chunk file and
// verifies that each chunk's data can be reassembled at the correct offset
// using the chunk_size returned from metadata.
//
// The key assertion: metadata must return the same chunk_size that was sent
// in CreateFile, so the download path can compute:
//   offset = chunkIndex * chunkSize
// If chunk_size is 0, all offsets collapse to 0 → data corruption.
func TestMultiChunkDownloadOffsetCorrectness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "cs-offset-file"
	chunkIDs := []string{"cs-offset-chunk-0", "cs-offset-chunk-1", "cs-offset-chunk-2"}
	payloads := [][]byte{
		[]byte("AAAAAAAAAA"), // 10 bytes — chunk 0
		[]byte("BBBBBBBBBB"), // 10 bytes — chunk 1
		[]byte("CCCCCCCCCC"), // 10 bytes — chunk 2
	}
	// Use a valid chunk size (minimum 64KB) — the actual payload per chunk
	// is small (10 bytes) but the chunk_size metadata must still be persisted
	// correctly for offset computation during download.
	var chunkSize int64 = 64 * 1024 // 64 KB — minimum valid size
	var totalSize int64 = 30

	// ── 1. CreateFile with chunk_size = 64KB ──
	createResp, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "offset-test.dat",
		FileSize:  totalSize,
		ChunkSize: chunkSize,
		ChunkIds:  chunkIDs,
	})
	require.NoError(t, err)
	require.Len(t, createResp.Placements, 3)

	// ── 2. Upload each chunk ──
	for i, pl := range createResp.Placements {
		primaryClient := DialStorage(t, pl.Primary.Address)
		var replicaAddrs []string
		for _, r := range pl.Replicas {
			replicaAddrs = append(replicaAddrs, r.Address)
		}
		PutChunkData(t, ctx, primaryClient, chunkIDs[i], fileID, payloads[i], replicaAddrs)
	}

	time.Sleep(1 * time.Second) // let commits propagate

	// ── 3. Verify chunk_size persisted ──
	getResp, err := testCluster.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
		FileId: fileID,
	})
	require.NoError(t, err)

	assert.Equal(t, chunkSize, getResp.File.ChunkSize,
		"chunk_size must be persisted so download can compute correct offsets")

	// ── 4. Verify: using the returned chunk_size, offsets are non-zero ──
	// This simulates what download.go does:
	//   offset = chunkIndex * chunkSize
	// If chunk_size is 0, all offsets collapse to 0 → data corruption.
	for _, ck := range getResp.Chunks {
		offset := int64(ck.ChunkIndex) * getResp.File.ChunkSize
		expectedOffset := int64(ck.ChunkIndex) * chunkSize
		assert.Equal(t, expectedOffset, offset,
			"chunk %d: offset should be %d, got %d (chunk_size=%d)",
			ck.ChunkIndex, expectedOffset, offset, getResp.File.ChunkSize)
	}

	// ── 5. Download each chunk and verify data integrity ──
	for i, pl := range createResp.Placements {
		client := DialStorage(t, pl.Primary.Address)
		data := GetChunkData(t, ctx, client, chunkIDs[i])
		assert.Equal(t, payloads[i], data, "chunk %d data mismatch", i)
	}
	t.Logf("offset correctness verified: chunk_size=%d, chunks=%d", getResp.File.ChunkSize, len(getResp.Chunks))
}

// ---------------------------------------------------------------------------
// Scenario: ChunkSize validation on the server
// ---------------------------------------------------------------------------

// TestCreateFileRejectsZeroChunkSize verifies the server rejects
// chunk_size <= 0 with InvalidArgument.
func TestCreateFileRejectsZeroChunkSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    "cs-zero-reject",
		FileName:  "bad-zero.dat",
		FileSize:  1024,
		ChunkSize: 0,
		ChunkIds:  []string{"cs-zero-chunk"},
	})
	require.Error(t, err, "CreateFile with chunk_size=0 should be rejected")
	st, ok := status.FromError(err)
	require.True(t, ok, "error should be a gRPC status")
	assert.Equal(t, codes.InvalidArgument, st.Code(),
		"expected InvalidArgument for chunk_size=0")
}

// TestCreateFileRejectsNegativeChunkSize verifies the server rejects
// negative chunk_size.
func TestCreateFileRejectsNegativeChunkSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    "cs-negative-reject",
		FileName:  "bad-neg.dat",
		FileSize:  1024,
		ChunkSize: -1,
		ChunkIds:  []string{"cs-neg-chunk"},
	})
	require.Error(t, err, "CreateFile with chunk_size=-1 should be rejected")
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// TestCreateFileRejectsOversizedChunkSize verifies the server rejects
// chunk_size exceeding the max (64 MB).
func TestCreateFileRejectsOversizedChunkSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    "cs-oversized-reject",
		FileName:  "bad-huge.dat",
		FileSize:  1024,
		ChunkSize: 128 * 1024 * 1024, // 128 MB — over the 64 MB limit
		ChunkIds:  []string{"cs-huge-chunk"},
	})
	require.Error(t, err, "CreateFile with chunk_size=128MB should be rejected")
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// TestCreateFileAcceptsValidChunkSizeRange verifies boundary values
// are accepted.
func TestCreateFileAcceptsValidChunkSizeRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tests := []struct {
		name      string
		fileID    string
		chunkSize int64
	}{
		{"min_64KB", "cs-min-ok", 64 * 1024},            // 64 KB — minimum
		{"1MB", "cs-1mb-ok", 1 * 1024 * 1024},           // 1 MB
		{"max_64MB", "cs-max-ok", 64 * 1024 * 1024},     // 64 MB — maximum
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
				FileId:    tc.fileID,
				FileName:  tc.fileID + ".dat",
				FileSize:  100 * 1024 * 1024,
				ChunkSize: tc.chunkSize,
				ChunkIds:  []string{tc.fileID + "-chunk-0"},
			})
			require.NoError(t, err, "CreateFile with chunk_size=%d should succeed", tc.chunkSize)
		})
	}
}

// TestCreateFileRejectsBelowMinChunkSize verifies the server rejects
// chunk_size below the minimum (64 KB).
func TestCreateFileRejectsBelowMinChunkSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := testCluster.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    "cs-too-small-reject",
		FileName:  "bad-small.dat",
		FileSize:  1024,
		ChunkSize: 1024, // 1 KB — below 64 KB minimum
		ChunkIds:  []string{"cs-small-chunk"},
	})
	require.Error(t, err, "CreateFile with chunk_size=1KB should be rejected")
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}
