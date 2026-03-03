package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type DiskStore struct {
	// rootDir is the base directory path where all chunks are stored
	rootDir string

	tempDir string

	// splitLevel determines the depth of directory sharding for chunk storage.
	// For example, with splitLevel=2, a chunk named "adwjij2jj424" is stored at
	// rootDir/ad/wj/adwjij2jj424.chunk, distributing chunks across subdirectories
	// to avoid filesystem performance issues with too many files in a single directory
	splitLevel uint16

	checksumStore ChecksumIndexStore[[32]byte]

	// stores totalspace in bytes
	// def = 4Gib => 4 * (2^30) bytes
	totalSpace uint64

	usedSpace uint64
}

// DiskStoreBuilder
type DiskStoreOptions func(*DiskStore)

var (
	// a compile-time check to ensure we implement the store interface correctly
	_ Store = (*DiskStore)(nil)
)

const (
	DEFAULT_STORE_SIZE = 4 * (1 << 30) // 4Gib in bytes
	DIR_SHARD_LEVEL    = 2
)

var (
	InvalidChunkId      = errors.New("chunk-id is not valid")
	ChunkNotFound       = errors.New("chunk not found")
	FailedToDeleteChunk = errors.New("chunk deletion failed")
	VerifyFailed        = errors.New("chunk mismatch checksum verification failed")
	InsufficientSpace   = errors.New("insufficient space")
)

// WithRootDir sets the root directory for the DiskStore.
func WithRootDir(dir string) DiskStoreOptions {
	return func(ds *DiskStore) {
		ds.rootDir = dir
	}
}

// WithTempDir sets the root directory for the DiskStore.
func WithTempDir(dir string) DiskStoreOptions {
	return func(ds *DiskStore) {
		ds.tempDir = dir
	}
}

// WithSplitLevel sets the directory sharding depth.
func WithSplitLevel(level uint16) DiskStoreOptions {
	return func(ds *DiskStore) {
		ds.splitLevel = level
	}
}

// WithTotalSpace sets the maximum space the store may use (in bytes).
func WithTotalSpace(bytes uint64) DiskStoreOptions {
	return func(ds *DiskStore) {
		ds.totalSpace = bytes
	}
}

// WithChecksumStore injects the checksum index implementation.
func WithChecksumStore(cs ChecksumIndexStore[[32]byte]) DiskStoreOptions {
	return func(ds *DiskStore) {
		ds.checksumStore = cs
	}
}

// NewDiskStore constructs a DiskStore with sensible defaults:
//   - rootDir = "." (current directory)
//   - splitLevel = DIR_SHARD_LEVEL (2)
//   - totalSpace = DEFAULT_STORE_SIZE (4 GiB)
//
// Callers must supply a checksumStore via WithChecksumStore and call
// checksumStore.Open() before using the DiskStore.
func NewDiskStore(opts ...DiskStoreOptions) (*DiskStore, error) {
	ds := &DiskStore{
		rootDir:    ".",
		tempDir:    ".",
		splitLevel: DIR_SHARD_LEVEL,
		totalSpace: DEFAULT_STORE_SIZE,
	}
	for _, opt := range opts {
		opt(ds)
	}
	err := os.MkdirAll(ds.rootDir, 0700)
	err = os.MkdirAll(ds.tempDir, 0700)

	if err != nil {
		return nil, err
	}
	return ds, nil
}

// Delete removes a chunk from the disk store.
// It removes the on-disk file, deletes the checksum index entry, and
// decrements usedSpace. Returns ChunkNotFound if the chunk does not exist.
func (ds *DiskStore) Delete(chunkId string) error {
	if chunkId == "" {
		return InvalidChunkId
	}

	if !ds.Exists(chunkId) {
		return ChunkNotFound
	}

	fullPath, err := ds.PathForChunk(chunkId)
	if err != nil {
		return err
	}
	chunkPath := filepath.Join(ds.rootDir, fullPath)

	info, err := os.Stat(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ChunkNotFound
		}
		return err
	}
	fileSize := uint64(info.Size())

	if err := os.Remove(chunkPath); err != nil {
		return fmt.Errorf("%w: %v", FailedToDeleteChunk, err)
	}

	// Remove from checksum index; best-effort — if this fails we log
	// but don't leave a deleted file referenced anymore.
	if err := ds.checksumStore.Delete(chunkId); err != nil && !errors.Is(err, ErrKeyNotFound) {
		// Non-fatal; file is already removed.
		_ = err
	}

	if ds.usedSpace >= fileSize {
		ds.usedSpace -= fileSize
	} else {
		ds.usedSpace = 0
	}

	return nil
}

// List returns all keys in the disk store
func (ds *DiskStore) List() ([]string, error) {
	return ds.checksumStore.GetAll()
}

// Exists checks if a key exists in the disk store
func (ds *DiskStore) Exists(chunkId string) bool {
	_, err := ds.checksumStore.Get(chunkId)
	return err == nil
}

// Write stores a value in the disk store
// Although this func is not to be used often
// Since we use ChunkWriter to write our frames
// To add entry to DiskStore
// We Should Use DiskStore.Move/Rename
func (ds *DiskStore) Write(chunkId string, value []byte) error {
	if chunkId == "" {
		return InvalidChunkId
	}

	// Check if we have enough space
	freeSpace, err := ds.FreeSpace()
	if err != nil {
		return err
	}
	if uint64(len(value)) > freeSpace {
		return InsufficientSpace
	}

	// Get the directory path for this chunk
	dirPath, err := getDirForChunkId(chunkId, ds.splitLevel)
	if err != nil {
		return err
	}

	fullDirPath := filepath.Join(ds.rootDir, dirPath)

	// Create directory structure if it doesn't exist
	if err := os.MkdirAll(fullDirPath, 0755); err != nil {
		return err
	}

	// Write the chunk file
	chunkPath := filepath.Join(fullDirPath, fmt.Sprintf("%s.%s", chunkId, "chunk"))
	if err := os.WriteFile(chunkPath, value, 0644); err != nil {
		return err
	}

	// Calculate and store checksum
	checksum := sha256.Sum256(value)
	if err := ds.checksumStore.Put(chunkId, checksum); err != nil {
		return err
	}

	// Update used space
	ds.usedSpace += uint64(len(value))

	return nil
}

// Rename takes the source file and renames it to the destination chunk path,
// then records the checksum and updates usedSpace.
//
// example:
//
//	from source = /store/temp/xyz.tmp
//	to dest     = rootDir/ab/sd/final.chunk
//
// This func is crucial to finalise a chunk written via ChunkWriter.
func (ds *DiskStore) Rename(source, chunkId string) error {
	if chunkId == "" {
		return InvalidChunkId
	}

	if _, err := os.Stat(source); err != nil {
		return err
	}

	relDest, err := ds.PathForChunk(chunkId)
	if err != nil {
		return err
	}
	dest := filepath.Join(ds.rootDir, relDest)
	destDir := filepath.Dir(dest)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	// fsync the source file before rename for durability.
	f, err := os.Open(source)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	if err := os.Rename(source, dest); err != nil {
		return err
	}

	// fsync the destination directory so the rename is visible after crash.
	if dirFd, err := os.Open(destDir); err == nil {
		_ = dirFd.Sync()
		_ = dirFd.Close()
	}

	// Read the newly-placed file to compute its checksum and size.
	data, err := os.ReadFile(dest)
	if err != nil {
		return err
	}
	checksum := sha256.Sum256(data)
	if err := ds.checksumStore.Put(chunkId, checksum); err != nil {
		return err
	}
	ds.usedSpace += uint64(len(data))

	return nil
}

// Read retrieves a value from the disk store
func (ds *DiskStore) Read(chunkId string) ([]byte, error) {
	if !ds.Exists(chunkId) {
		return nil, ChunkNotFound
	}

	res, err := ds.PathForChunk(chunkId)
	if err != nil {
		return nil, err
	}

	chunkPath := filepath.Join(ds.rootDir, res)

	data, err := os.ReadFile(chunkPath)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Verify checks the integrity of a key in the disk store
func (ds *DiskStore) Verify(chunkId string) error {
	val, err := ds.checksumStore.Get(chunkId)
	if err != nil {
		return err
	}
	data, err := ds.Read(chunkId)
	if err != nil {
		return err
	}
	calculatedHash := sha256.Sum256(data)

	if calculatedHash != val {
		return VerifyFailed
	}

	return nil
}

// TempDir returns the filepath for tempStoring of chunk
func (ds *DiskStore) TempDir(chunkId string) string {
	return filepath.Join(ds.tempDir, fmt.Sprintf("%s.%s", chunkId, ".tmp"))
}

// PathForChunk returns the full relative path (from rootDir) for a chunk.
// for example say for chunkid abcdefghijk... and shardLvl = 2
// it returns ab/cd/abcdefghijk....chunk
func (ds *DiskStore) PathForChunk(chunkId string) (string, error) {
	res, err := getDirForChunkId(chunkId, ds.splitLevel)
	if err != nil {
		return "", err
	}

	chunkPath := filepath.Join(res, fmt.Sprintf("%s.%s", chunkId, "chunk"))
	return chunkPath, nil
}

// FreeSpace returns the available space on disk
func (ds *DiskStore) FreeSpace() (uint64, error) {
	return max(ds.totalSpace-ds.usedSpace, 0), nil
}

// Size returns the total size used by the store
func (ds *DiskStore) Size() (uint64, error) {
	return ds.usedSpace, nil
}

// getDirForChunkId returns the required dir after sharding process
// for example say for chunkid abcdefghijk... and shardLvl = 2
// it returns ab/cd/
func getDirForChunkId(chunkId string, shardLvl uint16) (string, error) {
	if chunkId == "" {
		return "", InvalidChunkId
	}

	pathBuilder := strings.Builder{}
	cid := []byte(chunkId)
	if len(cid) < int(2*shardLvl) {
		return "", InvalidChunkId
	}

	for i := range shardLvl {
		pathBuilder.Write(cid[i*2 : i*2+2])
		pathBuilder.WriteByte('/')
	}

	return pathBuilder.String(), nil
}
