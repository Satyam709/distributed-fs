package chunk

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	dfserrors "github.com/satyam709/distributed-fs/internal/errors"
	"github.com/satyam709/distributed-fs/storage/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTestStore builds a wired-up DiskStore in a temp dir and registers cleanup.
func newTestStore(t *testing.T) store.Store {
	t.Helper()
	dir := t.TempDir()

	cs, err := store.NewChecksumIndexBoltDB[[]byte](store.ByteCodec{},
		store.WithDbPath[[]byte](dir),
	)
	require.NoError(t, err)
	require.NoError(t, cs.Open())
	t.Cleanup(cs.CleanUp)

	ds, err := store.NewDiskStore(
		store.WithRootDir(dir),
		store.WithTempDir(dir),
		store.WithSplitLevel(2),
		store.WithTotalSpace(64*1024*1024), // 64 MiB
		store.WithChecksumStore(cs),
	)
	require.NoError(t, err)
	return ds
}

// validChunkId must be ≥ 4 bytes for splitLevel=2.
const testChunkId = "deadbeef0123456789ab"

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestChunkWriter_HappyPath exercises the full write → finalize flow across
// two frames and verifies the chunk lands in the store with the right content.
func TestChunkWriter_HappyPath(t *testing.T) {
	s := newTestStore(t)

	frame1 := []byte("hello, ")
	frame2 := []byte("world!")
	payload := append(frame1, frame2...)
	expectedCS := sha256.Sum256(payload)

	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)

	require.NoError(t, w.Write(frame1))
	require.NoError(t, w.Write(frame2))
	require.NoError(t, w.Finalize(expectedCS[:]))

	// Chunk must be readable and bit-for-bit identical.
	got, err := s.Read(testChunkId)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

// TestChunkWriter_ChecksumMismatch ensures Finalize rejects a wrong checksum
// and cleans up the temp file.
func TestChunkWriter_ChecksumMismatch(t *testing.T) {
	s := newTestStore(t)

	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)

	require.NoError(t, w.Write([]byte("data")))

	tempPath := w.filepath

	wrongCs := make([]byte, 0)

	err = w.Finalize(wrongCs)
	assert.ErrorIs(t, err, dfserrors.ErrChecksumMismatch)

	// Temp file must be gone after abort.
	_, statErr := os.Stat(tempPath)
	assert.True(t, os.IsNotExist(statErr), "temp file should be removed after checksum mismatch")

	// Chunk must NOT be committed to the store.
	assert.False(t, s.Exists(testChunkId))
}

// TestChunkWriter_Abort_CleansUpTempFile verifies that Abort removes the
// partial temp file and the chunk is not visible in the store.
func TestChunkWriter_Abort_CleansUpTempFile(t *testing.T) {
	s := newTestStore(t)

	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)

	require.NoError(t, w.Write([]byte("partial data")))

	tempPath := w.filepath
	w.Abort()

	_, statErr := os.Stat(tempPath)
	assert.True(t, os.IsNotExist(statErr), "temp file should be removed by Abort")
	assert.False(t, s.Exists(testChunkId), "chunk must not appear in store after Abort")
}

// TestChunkWriter_WriteAfterFinalize ensures Write returns an error once the
// writer has been sealed (isDone = true).
func TestChunkWriter_WriteAfterFinalize(t *testing.T) {
	s := newTestStore(t)
	payload := []byte("sealed")

	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)

	require.NoError(t, w.Write(payload))
	cs := sha256.Sum256(payload)
	require.NoError(t, w.Finalize(cs[:]))

	// Subsequent Write must be rejected.
	writeErr := w.Write([]byte("extra"))
	assert.Error(t, writeErr, "Write after Finalize should return an error")
}

// TestChunkWriter_EmptyPayload verifies that a zero-byte chunk is accepted,
// committed correctly, and verifiable.
func TestChunkWriter_EmptyPayload(t *testing.T) {
	s := newTestStore(t)

	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)

	// No Write calls — finalize immediately with the empty-payload checksum.
	cs := sha256.Sum256([]byte{})
	require.NoError(t, w.Finalize(cs[:]))

	got, err := s.Read(testChunkId)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestChunkWriter_TempFileLocation checks that the temp file is created inside
// the store's temp directory (not the shard path) so cross-device rename works.
func TestChunkWriter_TempFileLocation(t *testing.T) {
	s := newTestStore(t)

	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)
	defer w.Abort()

	// TempDir for this chunk lives directly under the store tmp dir.
	tmpPath, err := s.TempDir(testChunkId)
	require.NoError(t, err)
	expectedDir := filepath.Dir(tmpPath)
	actualDir := filepath.Dir(w.filepath)
	assert.Equal(t, expectedDir, actualDir, "temp file should live in the store's temp directory")
}

// ---------------------------------------------------------------------------
// ChunkWriter edge cases
// ---------------------------------------------------------------------------

// TestChunkWriter_Abort_Idempotent ensures calling Abort twice doesn't panic.
func TestChunkWriter_Abort_Idempotent(t *testing.T) {
	s := newTestStore(t)
	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)
	w.Abort()
	assert.NotPanics(t, w.Abort, "second Abort must not panic")
}

// TestChunkWriter_WriteAfterAbort ensures Write returns an error after Abort
// (isDone=true blocks further writes).
func TestChunkWriter_WriteAfterAbort(t *testing.T) {
	s := newTestStore(t)
	w, err := NewChunkWriter(testChunkId, s)
	require.NoError(t, err)
	w.Abort()
	err = w.Write([]byte("should be rejected"))
	assert.ErrorIs(t, err, ErrWriterClosed, "Write after Abort must be rejected")
}

// TestChunkWriter_FinalizeAfterAbort verifies that Finalize after Abort
// does NOT commit the chunk, even with a correct checksum.
func TestChunkWriter_FinalizeAfterAbort(t *testing.T) {
	s := newTestStore(t)
	payload := []byte("abort then finalize")

	w, err := NewChunkWriter(testChunkId, s)

	require.NoError(t, err)
	require.NoError(t, w.Write(payload))

	w.Abort()
	cs := sha256.Sum256(payload)
	err = w.Finalize(cs[:])
	assert.ErrorIs(t, err, ErrOperationAborted)
	assert.False(t, s.Exists(testChunkId), "chunk must not be committed after Abort+Finalize")
}

// TestChunkWriter_LargePayload_MultipleFrames ensures the streaming checksum
// accumulation is correct across many frames.
func TestChunkWriter_LargePayload_MultipleFrames(t *testing.T) {
	const frameSize = 512
	const numFrames = 20

	s := newTestStore(t)
	w, err := NewChunkWriter(testChunkId, s)

	require.NoError(t, err)
	var full []byte

	for i := range numFrames {
		frame := make([]byte, frameSize)
		for j := range frame {
			frame[j] = byte(i)
		}
		full = append(full, frame...)
		require.NoError(t, w.Write(frame))
	}
	cs := sha256.Sum256(full)
	require.NoError(t, w.Finalize(cs[:]))

	got, err := s.Read(testChunkId)

	require.NoError(t, err)
	assert.Equal(t, full, got)
}
