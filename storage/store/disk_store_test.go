package store

import (
	"os"
	"path/filepath"
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

	cs := NewChecksumIndexBoltDB[[32]byte](
		WithDbPath[[32]byte](dir),
		WithCodec[[32]byte](sha256ArrayCodec{}),
	)
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

// sha256ArrayCodec marshals/unmarshals a [32]byte checksum.
type sha256ArrayCodec struct{}

func (sha256ArrayCodec) Marshal(v [32]byte) ([]byte, error) {
	b := make([]byte, 32)
	copy(b, v[:])
	return b, nil
}

func (sha256ArrayCodec) Unmarshal(b []byte) ([32]byte, error) {
	var arr [32]byte
	copy(arr[:], b)
	return arr, nil
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
	ds, _ := NewDiskStore()
	assert.Equal(t, ".", ds.rootDir, "default rootDir should be '.'")
	assert.Equal(t, uint16(DIR_SHARD_LEVEL), ds.splitLevel, "default splitLevel")
	assert.Equal(t, uint64(DEFAULT_STORE_SIZE), ds.totalSpace, "default totalSpace")
}

func TestNewDiskStore_WithOptions(t *testing.T) {
	ds, _ := NewDiskStore(
		WithRootDir("/tmp/mystore"),
		WithSplitLevel(3),
		WithTotalSpace(1024),
	)
	assert.Equal(t, "/tmp/mystore", ds.rootDir)
	assert.Equal(t, uint16(3), ds.splitLevel)
	assert.Equal(t, uint64(1024), ds.totalSpace)
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

	relPath, err := ds.PathForChunk(minChunkId)
	require.NoError(t, err)
	chunkPath := filepath.Join(ds.rootDir, relPath)

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
	assert.NoError(t, ds.Verify(minChunkId))

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

			relPath, err := ds.PathForChunk(minChunkId)
			require.NoError(t, err)
			chunkPath := filepath.Join(ds.rootDir, relPath)

			c.setup(ds, chunkPath)

			err = ds.Verify(minChunkId)
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
	err := ds.Verify("neverwritten12")
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
				s = NewChecksumIndexBoltDB[string](WithCodec(StringCodec{}))
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
