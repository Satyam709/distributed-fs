//go:build integration

package meta_storage

import (
	"context"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/integration/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Scenario 3: File metadata CRUD operations
// ---------------------------------------------------------------------------

// TestCreateFileDuplicate verifies that creating a file with an already-
// existing ID returns AlreadyExists.
func TestCreateFileDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileID := "dup-file-test"

	// First creation succeeds.
	_, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "duplicate.txt",
		FileSize:  42,
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{"dup-chunk-1"},
	})
	require.NoError(t, err, "first CreateFile should succeed")

	// Second creation with same ID fails.
	_, err = tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "duplicate-2.txt",
		FileSize:  100,
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{"dup-chunk-2"},
	})
	require.Error(t, err, "duplicate CreateFile should fail")
	st, ok := status.FromError(err)
	require.True(t, ok, "error should be a gRPC status")
	assert.Equal(t, codes.AlreadyExists, st.Code(),
		"expected AlreadyExists for duplicate file ID")
}

// TestListFiles verifies that ListFiles returns files that were created.
func TestListFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ids := []string{"list-file-a", "list-file-b"}
	for _, id := range ids {
		createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
			FileId:    id,
			FileName:  id + ".txt",
			FileSize:  1,
			ChunkSize: 64 * 1024,
			ChunkIds:  []string{id + "-chunk"},
		})
		require.NoError(t, err)

		primaryClient := testutil.DialStorage(t, createResp.Placements[0].Primary.Address)
		testutil.PutChunkData(t, ctx, primaryClient, id+"-chunk", id, []byte{42}, nil)
		time.Sleep(500 * time.Millisecond)

		_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
			FileId:   id,
			FileSize: 1,
		})
		require.NoError(t, err)
	}

	listResp, err := tc.MetaC.ListFiles(ctx, &pb_meta.ListFilesRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, listResp.Files, "ListFiles should return at least the files we created")

	found := make(map[string]bool)
	for _, f := range listResp.Files {
		found[f.FileId] = true
	}
	for _, id := range ids {
		assert.True(t, found[id], "expected file %q in ListFiles response", id)
	}
	t.Logf("ListFiles returned %d files total", len(listResp.Files))
}

// TestDeleteFile verifies that a file can be deleted and subsequently
// disappears from GetFile.
func TestDeleteFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileID := "delete-me-file"

	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "to-delete.txt",
		FileSize:  1,
		ChunkSize: 64 * 1024,
		ChunkIds:  []string{"delete-me-chunk"},
	})
	require.NoError(t, err)

	primaryClient := testutil.DialStorage(t, createResp.Placements[0].Primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, "delete-me-chunk", fileID, []byte{42}, nil)
	time.Sleep(500 * time.Millisecond)

	_, err = tc.MetaC.CommitFile(ctx, &pb_meta.CommitFileRequest{
		FileId:   fileID,
		FileSize: 1,
	})
	require.NoError(t, err)

	delResp, err := tc.MetaC.DeleteFile(ctx, &pb_meta.DeleteFileRequest{
		FileId: fileID,
	})
	require.NoError(t, err)
	assert.True(t, delResp.Success)

	getResp, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{FileId: fileID})
	require.Error(t, err, "GetFile should reject deleted files")
	t.Logf("GetFile after delete returns: %v", err)

	listResp, err := tc.MetaC.ListFiles(ctx, &pb_meta.ListFilesRequest{})
	require.NoError(t, err)
	for _, f := range listResp.Files {
		assert.NotEqual(t, fileID, f.FileId, "deleted file should not appear in ListFiles")
	}
	_ = getResp
}

// TestDeleteFileNotFound verifies deleting a non-existent file returns NotFound.
func TestDeleteFileNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := tc.MetaC.DeleteFile(ctx, &pb_meta.DeleteFileRequest{
		FileId: "no-such-file-ever",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

// TestGetFileNotFound verifies that GetFile for a non-existent ID returns NotFound.
func TestGetFileNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := tc.MetaC.GetFile(ctx, &pb_meta.GetFileRequest{
		FileId: "totally-bogus-id",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

// TestGetChunkLocationsAfterUpload verifies that after a successful upload
// the metadata node knows which storage nodes hold the chunk.
func TestGetChunkLocationsAfterUpload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "loc-test-file"
	chunkID := "loc-test-chunk"
	payload := []byte("data to track chunk locations for")

	// CreateFile.
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "locations.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)
	pl := createResp.Placements[0]

	var replicaAddrs []string
	for _, r := range pl.Replicas {
		replicaAddrs = append(replicaAddrs, r.Address)
	}

	// Upload to primary with fan-out.
	primaryClient := testutil.DialStorage(t, pl.Primary.Address)
	testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, replicaAddrs)

	// Wait for CommitChunk to propagate through Raft.
	time.Sleep(2 * time.Second)

	// GetChunkLocations should now return nodes.
	locResp, err := tc.MetaC.GetChunkLocations(ctx, &pb_meta.GetChunkLocationsRequest{
		ChunkId: chunkID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, locResp.Nodes, "chunk locations should contain at least the primary")

	t.Logf("chunk %s locations: %d nodes", chunkID, len(locResp.Nodes))
	for _, n := range locResp.Nodes {
		t.Logf("  node=%s addr=%s", n.NodeId, n.Address)
	}
}

// TestChunkVerifyAfterUpload verifies that a stored chunk passes integrity
// verification on the storage node.
func TestChunkVerifyAfterUpload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileID := "verify-test-file"
	chunkID := "verify-test-chunk"
	payload := []byte("integrity check data for integration test")

	// CreateFile.
	createResp, err := tc.MetaC.CreateFile(ctx, &pb_meta.CreateFileRequest{
		FileId:    fileID,
		FileName:  "verify.dat",
		FileSize:  int64(len(payload)),
		ChunkSize: 4 * 1024 * 1024,
		ChunkIds:  []string{chunkID},
	})
	require.NoError(t, err)
	pl := createResp.Placements[0]

	// Upload to primary.
	primaryClient := testutil.DialStorage(t, pl.Primary.Address)
	checksum := testutil.PutChunkData(t, ctx, primaryClient, chunkID, fileID, payload, nil)

	// VerifyChunk — try each storage node since we know which one is primary.
	var verifyResp *pb_storage.VerifyChunkResponse
	var verifyErr error
	for _, sc := range tc.StorageCs {
		verifyResp, verifyErr = sc.VerifyChunk(ctx, &pb_storage.VerifyChunkRequest{
			ChunkId:  chunkID,
			Checksum: checksum,
		})
		if verifyErr == nil {
			break
		}
	}
	require.NoError(t, verifyErr, "VerifyChunk should succeed on the node that has the chunk")
	assert.True(t, verifyResp.IsValid, "chunk should pass integrity verification")
}
