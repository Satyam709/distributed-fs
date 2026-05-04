//go:build integration

package meta_storage

import (
	"context"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/integration/testutil"
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

	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "chunksize-test.dat",
		FileSize:  10 * 1024 * 1024, // 10 MB file
		ChunkSize: chunkSize,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err, "CreateFile should succeed")

	primaryClient := testutil.DialStorage(t, createResp.Placements[0].Primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, []byte("chunks"), nil)
	time.Sleep(500 * time.Millisecond)

	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: 10 * 1024 * 1024,
	})
	require.NoError(t, err)

	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
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
		{"cs-diff-file-1mb", 1 * 1024 * 1024},   // 1 MB
		{"cs-diff-file-8mb", 8 * 1024 * 1024},   // 8 MB
		{"cs-diff-file-16mb", 16 * 1024 * 1024}, // 16 MB
	}

	for _, tt := range tests {
		createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
			FileId:    tt.fileID,
			FileName:  tt.fileID + ".dat",
			FileSize:  100 * 1024 * 1024, // 100 MB
			ChunkSize: tt.chunkSize,
			ChunkIds:  []string{tt.fileID + "-chunk-0"},
		})
		require.NoError(t, err, "CreateFile(%s) should succeed", tt.fileID)

		primaryClient := testutil.DialStorage(t, createResp.Placements[0].Primary.Address)
		testutil.PutChunkData(t, ctx, primaryClient, tt.fileID+"-chunk-0", tt.fileID, []byte("chunks"), nil)
		time.Sleep(500 * time.Millisecond)

		_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
			FileId:   tt.fileID,
			FileSize: 100 * 1024 * 1024,
		})
		require.NoError(t, err)
	}

	for _, tt := range tests {
		getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
			FileId: tt.fileID,
		})
		require.NoError(t, err, "GetFile(%s) should succeed", tt.fileID)
		assert.Equal(t, tt.chunkSize, getResp.File.ChunkSize,
			"file %s: ChunkSize mismatch", tt.fileID)
		t.Logf("file %s: expected=%d, got=%d ✓", tt.fileID, tt.chunkSize, getResp.File.ChunkSize)
	}
}

// TestChunkSizeInListFiles verifies that ChunkSize is also returned
// correctly in ListFiles responses.
func TestChunkSizeInListFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileID := "cs-list-file"
	var chunkSize int64 = 2 * 1024 * 1024 // 2 MB

	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "cs-list-test.dat",
		FileSize:  20 * 1024 * 1024,
		ChunkSize: chunkSize,
		ChunkIds:  []string{fileID + "-chunk-0"},
	})
	require.NoError(t, err)

	primaryClient := testutil.DialStorage(t, createResp.Placements[0].Primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, fileID+"-chunk-0", fileID, []byte("cs-list"), nil)
	time.Sleep(500 * time.Millisecond)

	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: 20 * 1024 * 1024,
	})
	require.NoError(t, err)

	listResp, err := tc.MetaC.ListFiles(ctx, &pb_meta.ListFilesRequest{})
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
//
//	offset = chunkIndex * chunkSize
//
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
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
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
		primaryClient := testutil.DialStorage(t, pl.Primary.Address)
		var replicaAddrs []string
		for _, r := range pl.Replicas {
			replicaAddrs = append(replicaAddrs, r.Address)
		}
		testutil.PutChunkData(t, ctx, primaryClient, chunkIDs[i], fileID, payloads[i], replicaAddrs)
	}

	time.Sleep(1 * time.Second) // let commits propagate

	// ── 2.5 Commit the file ──
	var allData []byte
	for _, p := range payloads {
		allData = append(allData, p...)
	}
	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: totalSize,
	})
	require.NoError(t, err)

	// ── 3. Verify chunk_size persisted ──
	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
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
		client := testutil.DialStorage(t, pl.Primary.Address)
		data := testutil.GetChunkData(t, ctx, client, chunkIDs[i])
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

	_, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
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

	_, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
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

	_, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
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
		{"min_64KB", "cs-min-ok", 64 * 1024},        // 64 KB — minimum
		{"1MB", "cs-1mb-ok", 1 * 1024 * 1024},       // 1 MB
		{"max_64MB", "cs-max-ok", 64 * 1024 * 1024}, // 64 MB — maximum
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
				FileId:    tt.fileID,
				FileName:  tt.fileID + ".dat",
				FileSize:  100 * 1024 * 1024,
				ChunkSize: tt.chunkSize,
				ChunkIds:  []string{tt.fileID + "-chunk-0"},
			})
			require.NoError(t, err, "CreateFile with chunk_size=%d should succeed", tt.chunkSize)
		})
	}
}

// TestCreateFileRejectsBelowMinChunkSize verifies the server rejects
// chunk_size below the minimum (64 KB).
func TestCreateFileRejectsBelowMinChunkSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
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
