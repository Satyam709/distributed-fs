package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newDiskStore creates a fully-wired DiskStore backed by a fresh temp directory
// and an opened BoltChecksumIndex. Both are cleaned up after the test.
func newTestDiskStore(t *testing.T, totalSpace uint64) *DiskStore {
	t.Helper()
	dir := t.TempDir()

	cs, err := NewChecksumIndexBoltDB[[]byte](ByteCodec{},
		WithDbPath[[]byte](dir),
	)
	require.NoError(t, err)

	require.NoError(t, cs.Open(), "checksum store Open() failed")
	t.Cleanup(cs.CleanUp)

	ds, _ := NewDiskStore(
		WithRootDir(dir),
		WithSplitLevel(2),
		WithTotalSpace(totalSpace),
		WithChecksumStore(cs),
	)
	return ds
}

// validChunkId returns a chunk id long enough for the default split level (2)
// so at least 4 chars are needed (2 * splitLevel).
const minChunkId = "abcdefgh1234"

// makeChunk fills a byte slice with a repeated byte value.
func makeChunk(size int, fill byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = fill
	}
	return b
}

// ---------------------------------------------------------------------------
// NewDiskStore constructor
// ---------------------------------------------------------------------------

func TestNewDiskStore_Defaults(t *testing.T) {
	cs, err := NewChecksumIndexBoltDB[[]byte](ByteCodec{}, WithDbPath[[]byte](t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, cs.Open())
	t.Cleanup(cs.CleanUp)

	ds, err := NewDiskStore(WithChecksumStore(cs))
	require.NoError(t, err)
	assert.Equal(t, ".", ds.rootDir, "default rootDir should be '.'")
	assert.Equal(t, uint16(DIR_SHARD_LEVEL), ds.splitLevel, "default splitLevel")
	assert.Equal(t, uint64(DEFAULT_STORE_SIZE), ds.totalSpace, "default totalSpace")
}

func TestNewDiskStore_WithOptions(t *testing.T) {
	cs, err := NewChecksumIndexBoltDB[[]byte](ByteCodec{}, WithDbPath[[]byte](t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, cs.Open())
	t.Cleanup(cs.CleanUp)

	ds, err := NewDiskStore(
		WithRootDir("/tmp/mystore"),
		WithSplitLevel(3),
		WithTotalSpace(1024),
		WithChecksumStore(cs),
	)
	require.NoError(t, err)
	assert.Equal(t, "/tmp/mystore", ds.rootDir)
	assert.Equal(t, uint16(3), ds.splitLevel)
	assert.Equal(t, uint64(1024), ds.totalSpace)
}

func TestNewDiskStore_NilChecksumStore_ReturnsError(t *testing.T) {
	_, err := NewDiskStore() // no WithChecksumStore
	require.Error(t, err)
	assert.Contains(t, err.Error(), "checksumStore is required")
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

func TestWrite(t *testing.T) {
	type tc struct {
		name       string
		chunkId    string
		data       []byte
		totalSpace uint64
		wantErr    error
	}

	cases := []tc{
		{
			name:       "valid write",
			chunkId:    minChunkId,
			data:       makeChunk(64, 'a'),
			totalSpace: 1024,
			wantErr:    nil,
		},
		{
			name:       "empty chunkId returns InvalidChunkId",
			chunkId:    "",
			data:       []byte("hello"),
			totalSpace: 1024,
			wantErr:    InvalidChunkId,
		},
		{
			name:       "too-short chunkId (fewer than 2*splitLevel bytes)",
			chunkId:    "ab",
			data:       []byte("hello"),
			totalSpace: 1024,
			wantErr:    InvalidChunkId,
		},
		{
			name:       "data larger than free space returns InsufficientSpace",
			chunkId:    minChunkId,
			data:       makeChunk(512, 'b'),
			totalSpace: 100,
			wantErr:    InsufficientSpace,
		},
		{
			name:       "zero-length data is allowed",
			chunkId:    minChunkId,
			data:       []byte{},
			totalSpace: 1024,
			wantErr:    nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ds := newTestDiskStore(t, c.totalSpace)
			err := ds.Write(c.chunkId, c.data)
			if c.wantErr != nil {
				assert.ErrorIs(t, err, c.wantErr)
			} else {
				require.NoError(t, err)
				assert.True(t, ds.Exists(c.chunkId), "chunk should exist after Write")
			}
		})
	}
}

func TestWrite_DuplicateOverwrites(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	first := makeChunk(32, 'x')
	second := makeChunk(32, 'y')

	require.NoError(t, ds.Write(minChunkId, first))
	require.NoError(t, ds.Write(minChunkId, second))

	got, err := ds.Read(minChunkId)
	require.NoError(t, err)
	assert.Equal(t, second, got, "second write should overwrite first")

	// usedSpace must not be doubled — only the live file size should count.
	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(len(second)), size, "usedSpace must not be doubled on overwrite")
}

func TestWrite_UpdatesUsedSpace(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	data := makeChunk(128, 'z')
	require.NoError(t, ds.Write(minChunkId, data))
	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(128), size)
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

func TestRead(t *testing.T) {
	type tc struct {
		name    string
		setup   func(ds *DiskStore)
		chunkId string
		wantErr error
		wantNil bool
	}

	data := makeChunk(64, 'm')

	cases := []tc{
		{
			name: "read existing chunk returns data",
			setup: func(ds *DiskStore) {
				require.NoError(t, ds.Write(minChunkId, data))
			},
			chunkId: minChunkId,
			wantErr: nil,
		},
		{
			name:    "read missing chunk returns ChunkNotFound",
			setup:   func(ds *DiskStore) {},
			chunkId: minChunkId,
			wantErr: ChunkNotFound,
			wantNil: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ds := newTestDiskStore(t, 1024*1024)
			c.setup(ds)

			got, err := ds.Read(c.chunkId)
			if c.wantErr != nil {
				assert.ErrorIs(t, err, c.wantErr)
				if c.wantNil {
					assert.Nil(t, got)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, data, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Exists
// ---------------------------------------------------------------------------

func TestExists(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)

	assert.False(t, ds.Exists(minChunkId), "chunk should not exist before Write")
	require.NoError(t, ds.Write(minChunkId, makeChunk(16, 'e')))
	assert.True(t, ds.Exists(minChunkId), "chunk should exist after Write")
}

func TestExists_ZeroChecksum_DoesNotFalsePositive(t *testing.T) {
	// This tests the old buggy `len(val) != 0` path: [32]byte{} has 32 bytes
	// so the old code would have returned true even for the zero value.
	// With the fix (`err == nil`), only keys actually stored in the index
	// should return true.
	ds := newTestDiskStore(t, 1024*1024)
	assert.False(t, ds.Exists("neverwritten12"), "non-existent chunk must return false")
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestDelete(t *testing.T) {
	type tc struct {
		name    string
		setup   func(ds *DiskStore)
		chunkId string
		wantErr error
	}

	data := makeChunk(64, 'd')

	cases := []tc{
		{
			name: "delete existing chunk succeeds",
			setup: func(ds *DiskStore) {
				require.NoError(t, ds.Write(minChunkId, data))
			},
			chunkId: minChunkId,
			wantErr: nil,
		},
		{
			name:    "delete non-existent chunk returns ChunkNotFound",
			setup:   func(ds *DiskStore) {},
			chunkId: minChunkId,
			wantErr: ChunkNotFound,
		},
		{
			name:    "delete with empty chunkId returns InvalidChunkId",
			setup:   func(ds *DiskStore) {},
			chunkId: "",
			wantErr: InvalidChunkId,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ds := newTestDiskStore(t, 1024*1024)
			c.setup(ds)

			err := ds.Delete(c.chunkId)
			if c.wantErr != nil {
				assert.ErrorIs(t, err, c.wantErr)
			} else {
				require.NoError(t, err)
				assert.False(t, ds.Exists(c.chunkId), "chunk should not exist after Delete")
			}
		})
	}
}

func TestDelete_RemovesChecksumEntry(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	require.NoError(t, ds.Write(minChunkId, makeChunk(32, 'r')))
	require.NoError(t, ds.Delete(minChunkId))

	keys, err := ds.List()
	require.NoError(t, err)
	assert.NotContains(t, keys, minChunkId, "checksum index should not contain deleted chunk")
}

func TestDelete_DecrementsUsedSpace(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	data := makeChunk(128, 's')
	require.NoError(t, ds.Write(minChunkId, data))

	sizeBefore, _ := ds.Size()
	require.NoError(t, ds.Delete(minChunkId))
	sizeAfter, _ := ds.Size()

	assert.Equal(t, uint64(128), sizeBefore)
	assert.Equal(t, uint64(0), sizeAfter, "usedSpace should be zero after deleting the only chunk")
}

func TestDelete_RemovesFileFromDisk(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	require.NoError(t, ds.Write(minChunkId, makeChunk(32, 'f')))

	chunkPath, err := ds.PathForChunk(minChunkId)
	require.NoError(t, err)

	_, statErr := os.Stat(chunkPath)
	require.NoError(t, statErr, "file should exist before delete")

	require.NoError(t, ds.Delete(minChunkId))
	_, statErr = os.Stat(chunkPath)
	assert.True(t, os.IsNotExist(statErr), "file should be removed from disk after delete")
}

// ---------------------------------------------------------------------------
// Rename
// ---------------------------------------------------------------------------

func TestRename(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	data := makeChunk(128, 'n')

	// Create a temp source file
	tmpFile, err := os.CreateTemp(ds.rootDir, "*.tmp")
	require.NoError(t, err)
	_, err = tmpFile.Write(data)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	require.NoError(t, ds.Rename(tmpFile.Name(), minChunkId))

	// File should now be tracked
	assert.True(t, ds.Exists(minChunkId), "chunk should exist after Rename")

	// Checksum should be correct
	_, err = ds.Verify(minChunkId)
	assert.NoError(t, err)

	// usedSpace should reflect file size
	size, _ := ds.Size()
	assert.Equal(t, uint64(len(data)), size)
}

func TestRename_MissingSource_ReturnsError(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	err := ds.Rename("/nonexistent/path/file.tmp", minChunkId)
	assert.Error(t, err)
}

func TestRename_EmptyChunkId_ReturnsInvalidChunkId(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	tmpFile, err := os.CreateTemp(ds.rootDir, "*.tmp")
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	err = ds.Rename(tmpFile.Name(), "")
	assert.ErrorIs(t, err, InvalidChunkId)
}

func TestRename_TooShortChunkId_ReturnsInvalidChunkId(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	tmpFile, err := os.CreateTemp(ds.rootDir, "*.tmp")
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	err = ds.Rename(tmpFile.Name(), "ab") // needs >= 4 chars for splitLevel=2
	assert.ErrorIs(t, err, InvalidChunkId)
}

// ---------------------------------------------------------------------------
// Verify
// ---------------------------------------------------------------------------

func TestVerify(t *testing.T) {
	type tc struct {
		name    string
		setup   func(ds *DiskStore, chunkPath string)
		wantErr error
	}

	cases := []tc{
		{
			name:    "valid chunk passes verification",
			setup:   func(ds *DiskStore, chunkPath string) {},
			wantErr: nil,
		},
		{
			name: "tampered file returns VerifyFailed",
			setup: func(ds *DiskStore, chunkPath string) {
				// Overwrite file contents without updating checksum
				require.NoError(t, os.WriteFile(chunkPath, []byte("TAMPERED"), 0644))
			},
			wantErr: VerifyFailed,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ds := newTestDiskStore(t, 1024*1024)
			data := makeChunk(64, 'v')
			require.NoError(t, ds.Write(minChunkId, data))

			chunkPath, err := ds.PathForChunk(minChunkId)
			require.NoError(t, err)

			c.setup(ds, chunkPath)

			_, err = ds.Verify(minChunkId)
			if c.wantErr != nil {
				assert.ErrorIs(t, err, c.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestVerify_MissingChunk_ReturnsError(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)
	_, err := ds.Verify("neverwritten12")
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

func TestList(t *testing.T) {
	type tc struct {
		name    string
		chunks  map[string][]byte
		wantLen int
		wantIds []string
	}

	cases := []tc{
		{
			name:    "empty store returns empty list",
			chunks:  map[string][]byte{},
			wantLen: 0,
			wantIds: []string{},
		},
		{
			name: "multiple writes appear in list",
			chunks: map[string][]byte{
				"aabbccdd1122": makeChunk(16, '1'),
				"bbccddee2233": makeChunk(16, '2'),
				"ccddeeff3344": makeChunk(16, '3'),
			},
			wantLen: 3,
			wantIds: []string{"aabbccdd1122", "bbccddee2233", "ccddeeff3344"},
		},
		{
			name: "deleted chunk absent from list",
			chunks: map[string][]byte{
				"aabbccdd1122": makeChunk(16, '4'),
			},
			wantLen: 0, // will be deleted in test body
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ds := newTestDiskStore(t, 1024*1024)
			for id, data := range c.chunks {
				require.NoError(t, ds.Write(id, data))
			}

			// For the "deleted chunk" case, delete what we wrote.
			if c.name == "deleted chunk absent from list" {
				for id := range c.chunks {
					require.NoError(t, ds.Delete(id))
				}
			}

			keys, err := ds.List()
			require.NoError(t, err)
			assert.Len(t, keys, c.wantLen)
			if len(c.wantIds) > 0 {
				assert.ElementsMatch(t, c.wantIds, keys)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FreeSpace / Size
// ---------------------------------------------------------------------------

func TestFreeSpaceAndSize(t *testing.T) {
	const total = uint64(1024)
	ds := newTestDiskStore(t, total)

	free, err := ds.FreeSpace()
	require.NoError(t, err)
	assert.Equal(t, total, free, "initially all space should be free")

	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), size, "initially used size should be 0")

	data := makeChunk(256, 'q')
	require.NoError(t, ds.Write(minChunkId, data))

	free, _ = ds.FreeSpace()
	size, _ = ds.Size()
	assert.Equal(t, uint64(256), size)
	assert.Equal(t, total-256, free)

	require.NoError(t, ds.Delete(minChunkId))
	free, _ = ds.FreeSpace()
	size, _ = ds.Size()
	assert.Equal(t, uint64(0), size, "usedSpace should be zero after delete")
	assert.Equal(t, total, free, "all space should be free after delete")
}

// ---------------------------------------------------------------------------
// getDirForChunkId (extended helpers)
// ---------------------------------------------------------------------------

func TestGetDirForChunkId_Extended(t *testing.T) {
	type tc struct {
		name       string
		chunkId    string
		shardLvl   uint16
		wantResult string
		wantErr    error
	}

	cases := []tc{
		{
			name:       "shardLvl=0 returns empty string",
			chunkId:    "abcdef",
			shardLvl:   0,
			wantResult: "",
			wantErr:    nil,
		},
		{
			name:       "shardLvl=1 requires 2 chars minimum",
			chunkId:    "ab",
			shardLvl:   1,
			wantResult: "ab/",
			wantErr:    nil,
		},
		{
			name:     "chunkId too short for shardLvl=1 needs >=2 chars",
			chunkId:  "a",
			shardLvl: 1,
			wantErr:  InvalidChunkId,
		},
		{
			name:       "shardLvl=2 standard",
			chunkId:    "abcdefgh",
			shardLvl:   2,
			wantResult: "ab/cd/",
			wantErr:    nil,
		},
		{
			name:     "empty chunkId always returns InvalidChunkId",
			chunkId:  "",
			shardLvl: 0,
			wantErr:  InvalidChunkId,
		},
		{
			name:     "chunkId shorter than 2*shardLvl returns InvalidChunkId",
			chunkId:  "abc",
			shardLvl: 2,
			wantErr:  InvalidChunkId,
		},
		{
			name:       "shardLvl=5 large sharding",
			chunkId:    "abcdefghij",
			shardLvl:   5,
			wantResult: "ab/cd/ef/gh/ij/",
			wantErr:    nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := getDirForChunkId(c.chunkId, c.shardLvl)
			assert.ErrorIs(t, err, c.wantErr)
			if c.wantErr == nil {
				assert.Equal(t, c.wantResult, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Checksum index Delete method
// ---------------------------------------------------------------------------

func TestChecksumIndex_Delete(t *testing.T) {
	type tc struct {
		name    string
		setup   func(s *BoltChecksumIndex[string])
		key     string
		wantErr error
	}

	cases := []tc{
		{
			name: "delete existing key succeeds",
			setup: func(s *BoltChecksumIndex[string]) {
				require.NoError(t, s.Put("mykey", "value"))
			},
			key:     "mykey",
			wantErr: nil,
		},
		{
			name:    "delete non-existent key returns ErrKeyNotFound",
			setup:   func(s *BoltChecksumIndex[string]) {},
			key:     "ghost",
			wantErr: ErrKeyNotFound,
		},
		{
			name:    "delete with empty key returns ErrEmptyKey",
			setup:   func(s *BoltChecksumIndex[string]) {},
			key:     "",
			wantErr: ErrEmptyKey,
		},
		{
			name:    "delete before Open returns ErrDatabaseNotOpened",
			setup:   nil, // signals we use an unopened store
			key:     "k",
			wantErr: ErrDatabaseNotOpened,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var s *BoltChecksumIndex[string]
			if c.setup == nil {
				var err error
				s, err = NewChecksumIndexBoltDB[string](StringCodec{})
				require.NoError(t, err)
			} else {
				s = newOpenedStore(t)
				c.setup(s)
			}

			err := s.Delete(c.key)
			if c.wantErr != nil {
				assert.ErrorIs(t, err, c.wantErr)
			} else {
				require.NoError(t, err)
				// Confirm key is gone
				_, getErr := s.Get(c.key)
				assert.ErrorIs(t, getErr, ErrKeyNotFound, "key should be gone after Delete")
			}
		})
	}
}

func TestChecksumIndex_Delete_KeyGoneFromGetAll(t *testing.T) {
	s := newOpenedStore(t)
	require.NoError(t, s.Put("a", "1"))
	require.NoError(t, s.Put("b", "2"))
	require.NoError(t, s.Delete("a"))

	keys, err := s.GetAll()
	require.NoError(t, err)
	assert.NotContains(t, keys, "a")
	assert.Contains(t, keys, "b")
}

// ---------------------------------------------------------------------------
// Helper — tempDir same filesystem as rootDir (required for os.Rename)
// ---------------------------------------------------------------------------

// newTestDiskStoreWithTemp is like newTestDiskStore but sets tempDir == rootDir
// so that os.Rename works without cross-device link errors.
func newTestDiskStoreWithTemp(t *testing.T, totalSpace uint64) *DiskStore {
	t.Helper()
	dir := t.TempDir()
	cs, err := NewChecksumIndexBoltDB[[]byte](ByteCodec{},
		WithDbPath[[]byte](dir),
	)
	require.NoError(t, err)
	require.NoError(t, cs.Open(), "checksum store Open() failed")
	t.Cleanup(cs.CleanUp)
	ds, err := NewDiskStore(
		WithRootDir(dir),
		WithTempDir(dir),
		WithSplitLevel(2),
		WithTotalSpace(totalSpace),
		WithChecksumStore(cs),
	)
	require.NoError(t, err)
	return ds
}

// ---------------------------------------------------------------------------
// TempDir
// ---------------------------------------------------------------------------

// TestTempDir_PathFormat verifies TempDir returns a path inside ds.tempDir
// that includes the chunkId.
func TestTempDir_PathFormat(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	path, err := ds.TempDir(minChunkId)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(filepath.Clean(path), filepath.Clean(ds.tempDir)),
		"TempDir path %q should be under ds.tempDir %q", path, ds.tempDir)
	assert.Contains(t, path, minChunkId)
}

// TestTempDir_DifferentChunksGetDifferentPaths ensures two distinct chunkIds
// produce distinct temp paths (concurrent writes must not collide).
func TestTempDir_DifferentChunksGetDifferentPaths(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	p1, err := ds.TempDir("aabbccddeeff")
	require.NoError(t, err)
	p2, err := ds.TempDir("bbccddeeff00")
	require.NoError(t, err)
	assert.NotEqual(t, p1, p2)
}

// ---------------------------------------------------------------------------
// PathForChunk
// ---------------------------------------------------------------------------

// TestPathForChunk_CorrectShardPath checks the absolute shard path for splitLevel=2.
func TestPathForChunk_CorrectShardPath(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	got, err := ds.PathForChunk("abcdefgh1234")
	require.NoError(t, err)
	// PathForChunk now returns an absolute path: rootDir + shard dirs + filename.
	want := filepath.Join(ds.rootDir, "ab", "cd", "abcdefgh1234.chunk")
	assert.Equal(t, want, got)
}

func TestPathForChunk_EmptyChunkId_ReturnsError(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	_, err := ds.PathForChunk("")
	assert.ErrorIs(t, err, InvalidChunkId)
}

func TestPathForChunk_TooShortChunkId_ReturnsError(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	_, err := ds.PathForChunk("ab") // needs ≥ 4 chars for splitLevel=2
	assert.ErrorIs(t, err, InvalidChunkId)
}

// TestRename_ChunkAvailableForRead ensures a Rename-committed chunk reads back
// with the original bytes.
func TestRename_ChunkAvailableForRead(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	data := makeChunk(64, 'r')
	tmp, err := os.CreateTemp(ds.tempDir, "*.tmp")
	require.NoError(t, err)
	_, err = tmp.Write(data)
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	require.NoError(t, ds.Rename(tmp.Name(), minChunkId))
	got, err := ds.Read(minChunkId)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// TestRename_SourceGoneAfterRename confirms the temp file is gone after a
// successful rename (os.Rename moves, not copies).
func TestRename_SourceGoneAfterRename(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	tmp, err := os.CreateTemp(ds.tempDir, "*.tmp")
	require.NoError(t, err)
	_, err = tmp.Write(makeChunk(32, 'x'))
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	tmpName := tmp.Name()
	require.NoError(t, ds.Rename(tmpName, minChunkId))
	_, statErr := os.Stat(tmpName)
	assert.True(t, os.IsNotExist(statErr), "source temp file should be gone after Rename")
}

// TestDelete_UsedSpaceDoesNotUnderflow checks the clamp in Delete: if
// usedSpace is already 0 when a file is deleted, it must stay 0, not wrap.
func TestDelete_UsedSpaceDoesNotUnderflow(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)
	require.NoError(t, ds.Write(minChunkId, makeChunk(128, 'u')))
	ds.usedSpace = 0 // force underflow condition
	require.NoError(t, ds.Delete(minChunkId))
	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), size, "usedSpace must clamp to 0, not underflow")
}

// TestWrite_OverwriteAccountsDelta writes a large chunk then overwrites with a
// smaller one and confirms usedSpace tracks the *net* size, not an accumulated sum.
func TestWrite_OverwriteAccountsDelta(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)

	// First write: 256 bytes.
	require.NoError(t, ds.Write(minChunkId, makeChunk(256, 'a')))
	size, _ := ds.Size()
	assert.Equal(t, uint64(256), size)

	// Overwrite with 128 bytes — usedSpace should shrink, NOT grow to 384.
	require.NoError(t, ds.Write(minChunkId, makeChunk(128, 'b')))
	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(128), size, "usedSpace should equal newSize after shrinking overwrite")

	// Overwrite with a larger chunk — usedSpace should grow to 512.
	require.NoError(t, ds.Write(minChunkId, makeChunk(512, 'c')))
	size, err = ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(512), size, "usedSpace should equal newSize after growing overwrite")
}

// TestWrite_Overwrite_InsufficientSpace ensures that even for an overwrite, the
// *net* increase is checked: if the new data is larger than old + freeSpace,
// we must get InsufficientSpace.
func TestWrite_Overwrite_InsufficientSpace(t *testing.T) {
	// Total space = 200 bytes.
	ds := newTestDiskStore(t, 200)

	// Write 100 bytes → 100 bytes free.
	require.NoError(t, ds.Write(minChunkId, makeChunk(100, 'a')))

	// Attempt to overwrite with 201 bytes: net delta = 101 > 100 free.
	err := ds.Write(minChunkId, makeChunk(201, 'b'))
	assert.ErrorIs(t, err, InsufficientSpace)

	// Overwrite with 150 bytes: net delta = 50 ≤ 100 free — should succeed.
	require.NoError(t, ds.Write(minChunkId, makeChunk(150, 'c')))
}

// TestRename_OverwriteAccountsDelta commits a chunk via Rename, then re-renames
// into the same chunkId with a different size and verifies usedSpace is the
// size of the *current* file, not an accumulated sum.
func TestRename_OverwriteAccountsDelta(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1<<20)

	// Helper: write payload to a temp file and return its path.
	writeTmp := func(data []byte) string {
		tmp, err := os.CreateTemp(ds.tempDir, "*.tmp")
		require.NoError(t, err)
		_, err = tmp.Write(data)
		require.NoError(t, err)
		require.NoError(t, tmp.Close())
		return tmp.Name()
	}

	// First Rename: 256 bytes.
	require.NoError(t, ds.Rename(writeTmp(makeChunk(256, 'a')), minChunkId))
	size, _ := ds.Size()
	assert.Equal(t, uint64(256), size)

	// Second Rename (overwrite) with 128 bytes — usedSpace must NOT be 384.
	require.NoError(t, ds.Rename(writeTmp(makeChunk(128, 'b')), minChunkId))
	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(128), size, "usedSpace should equal new file size after shrinking Rename overwrite")

	// Third Rename (overwrite) with 512 bytes.
	require.NoError(t, ds.Rename(writeTmp(makeChunk(512, 'c')), minChunkId))
	size, err = ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(512), size, "usedSpace should equal new file size after growing Rename overwrite")
}

// TestRename_InsufficientSpace checks that Rename returns InsufficientSpace
// when the net increase would exceed totalSpace.
func TestRename_InsufficientSpace(t *testing.T) {
	// Total = 200 bytes. Write a 100-byte chunk.
	ds := newTestDiskStoreWithTemp(t, 200)
	require.NoError(t, ds.Write(minChunkId, makeChunk(100, 'a')))

	// Now try to Rename a 201-byte file into the same chunkId.
	// net delta = 201 - 100 = 101 > 100 free.
	tmp, err := os.CreateTemp(ds.tempDir, "*.tmp")
	require.NoError(t, err)
	_, err = tmp.Write(makeChunk(201, 'z'))
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	err = ds.Rename(tmp.Name(), minChunkId)
	// Temp file cleanup: it may or may not exist depending on when the check fires.
	_ = os.Remove(tmp.Name())

	assert.ErrorIs(t, err, InsufficientSpace)
}

// TestChunkId_PathTraversal_Write ensures Write rejects chunkIds that contain
// path separators or ".." dot-segments.
func TestChunkId_PathTraversal_Write(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)

	traversalIds := []string{
		"../evil",
		"../../etc/passwd",
		"abcd/efgh",
		"abcd\\efgh",
		"ab..cd1234",
	}

	for _, id := range traversalIds {
		t.Run(id, func(t *testing.T) {
			err := ds.Write(id, []byte("payload"))
			assert.ErrorIs(t, err, InvalidChunkId, "chunkId %q should be rejected as invalid", id)
		})
	}
}

// TestChunkId_PathTraversal_Rename ensures Rename rejects traversal chunkIds.
func TestChunkId_PathTraversal_Rename(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1024*1024)

	tmp, err := os.CreateTemp(ds.tempDir, "*.tmp")
	require.NoError(t, err)
	_, err = tmp.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	traversalIds := []string{
		"../evil",
		"abcd/efgh",
		"ab..cd1234",
	}

	for _, id := range traversalIds {
		t.Run(id, func(t *testing.T) {
			// Re-create the temp file for each iteration since Rename may move it.
			tmp2, err := os.CreateTemp(ds.tempDir, "*.tmp")
			require.NoError(t, err)
			_, err = tmp2.Write([]byte("data"))
			require.NoError(t, err)
			require.NoError(t, tmp2.Close())

			err = ds.Rename(tmp2.Name(), id)
			_ = os.Remove(tmp2.Name()) // cleanup if not moved
			assert.ErrorIs(t, err, InvalidChunkId, "chunkId %q should be rejected as invalid", id)
		})
	}
}

// TestChunkId_PathTraversal_Read ensures Read returns ChunkNotFound (not a path
// escape) when given a traversal chunkId.
func TestChunkId_PathTraversal_Read(t *testing.T) {
	ds := newTestDiskStore(t, 1024*1024)

	traversalIds := []string{
		"../somefile",
		"ab/cdef1234",
	}

	for _, id := range traversalIds {
		t.Run(id, func(t *testing.T) {
			data, err := ds.Read(id)
			assert.Nil(t, data)
			assert.ErrorIs(t, err, ChunkNotFound,
				"Read with traversal chunkId should return ChunkNotFound, got: %v", err)
		})
	}
}

// TestChunkId_PathTraversal_TempDir ensures TempDir returns an error instead
// of constructing an escaped path.
func TestChunkId_PathTraversal_TempDir(t *testing.T) {
	ds := newTestDiskStoreWithTemp(t, 1024*1024)

	traversalIds := []string{
		"../evil",
		"ab/cdef1234",
		"ab..cd5678",
	}

	for _, id := range traversalIds {
		t.Run(id, func(t *testing.T) {
			path, err := ds.TempDir(id)
			assert.Empty(t, path)
			assert.ErrorIs(t, err, InvalidChunkId,
				"TempDir with traversal chunkId should return InvalidChunkId")
		})
	}
}

// TestConcurrent_Write_UsedSpace runs N goroutines each writing a unique chunk
// concurrently. Run with -race to detect data races. After all writes the
// total usedSpace must equal N * chunkSize.
func TestConcurrent_Write_UsedSpace(t *testing.T) {
	const n = 50
	const chunkSize = 256
	// totalSpace must accommodate all concurrent writes.
	ds := newTestDiskStore(t, n*chunkSize*2)

	ids := make([]string, n)
	for i := range ids {
		// Each id must be unique and ≥ 4 chars for splitLevel=2.
		ids[i] = strings.ToLower(makeHexId(i))
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(idx int) {
			defer wg.Done()
			err := ds.Write(ids[idx], makeChunk(chunkSize, byte(idx)))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(n*chunkSize), size,
		"usedSpace must equal n*chunkSize after concurrent writes")
}

// TestConcurrent_Delete_UsedSpace writes N chunks sequentially then deletes
// them all concurrently. Final usedSpace must be 0.
func TestConcurrent_Delete_UsedSpace(t *testing.T) {
	const n = 50
	const chunkSize = 256
	ds := newTestDiskStore(t, n*chunkSize*2)

	ids := make([]string, n)
	for i := range ids {
		ids[i] = strings.ToLower(makeHexId(i))
		require.NoError(t, ds.Write(ids[i], makeChunk(chunkSize, byte(i))))
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(idx int) {
			defer wg.Done()
			err := ds.Delete(ids[idx])
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	size, err := ds.Size()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), size, "usedSpace must be 0 after concurrent deletes")
}

// makeHexId produces a deterministic, lowercase hex-like chunk id from an
// integer that is long enough for the default splitLevel=2 (≥4 chars).
func makeHexId(n int) string {
	return fmt.Sprintf("chunk%08x", n)
}
