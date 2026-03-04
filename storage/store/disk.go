package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	dfserrors "github.com/satyam709/distributed-fs/internal/errors"
	"github.com/satyam709/distributed-fs/internal/logging"
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

	checksumStore ChecksumIndexStore[[]byte]

	// stores totalspace in bytes
	// def = 4Gib => 4 * (2^30) bytes
	totalSpace uint64

	// mu guards usedSpace and all multi-step operations that read-then-modify it.
	// Write lock: Write, Rename, Delete (they mutate usedSpace).
	// Read lock:  FreeSpace, Size.
	mu        sync.RWMutex
	usedSpace uint64

	logger *logging.CLogger
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

// Sentinel errors — canonical definitions live in internal/errors; these
// aliases are kept for backward-compatibility with existing store-internal
// code (e.g. disk_store_test.go).
var (
	InvalidChunkId      = dfserrors.ErrInvalidChunkId
	ChunkNotFound       = dfserrors.ErrChunkNotFound
	FailedToDeleteChunk = dfserrors.ErrFailedToDelete
	VerifyFailed        = dfserrors.ErrVerifyFailed
	InsufficientSpace   = dfserrors.ErrInsufficientSpace
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
func WithChecksumStore(cs ChecksumIndexStore[[]byte]) DiskStoreOptions {
	return func(ds *DiskStore) {
		ds.checksumStore = cs
	}
}

// WithDiskStoreLogger injects a logger into the DiskStore.
func WithDiskStoreLogger(l *logging.CLogger) DiskStoreOptions {
	return func(ds *DiskStore) {
		ds.logger = l
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
		logger:     logging.NewCLogger(),
	}
	ds.logger.Logger = *ds.logger.Logger.With(slog.String("component", "DiskStore"))

	for _, opt := range opts {
		opt(ds)
	}

	ds.logger.Info("initialising DiskStore",
		slog.String("rootDir", ds.rootDir),
		slog.String("tempDir", ds.tempDir),
		slog.Uint64("totalSpaceBytes", ds.totalSpace),
		slog.Int("splitLevel", int(ds.splitLevel)),
	)

	if err := os.MkdirAll(ds.rootDir, 0700); err != nil {
		ds.logger.Error("failed to create rootDir", err, slog.String("path", ds.rootDir))
		return nil, err
	}
	if err := os.MkdirAll(ds.tempDir, 0700); err != nil {
		ds.logger.Error("failed to create tempDir", err, slog.String("path", ds.tempDir))
		return nil, err
	}

	if ds.checksumStore == nil {
		return nil, errors.New("DiskStore: checksumStore is required — use WithChecksumStore()")
	}

	ds.logger.Info("DiskStore ready")
	return ds, nil
}

// validateChunkId rejects chunk identifiers that are:
//   - empty
//   - shorter than 2*splitLevel (needed for shard path construction)
//   - containing path-separator characters ('/', '\') or the ".." sequence
//
// Any of these conditions returns InvalidChunkId.
func (ds *DiskStore) validateChunkId(chunkId string) error {
	if chunkId == "" {
		return InvalidChunkId
	}
	if len(chunkId) < int(2*ds.splitLevel) {
		return InvalidChunkId
	}
	// Guard against path traversal: reject any '/', '\', or ".." segment.
	if strings.ContainsAny(chunkId, "/\\") || strings.Contains(chunkId, "..") {
		return InvalidChunkId
	}
	return nil
}

// Delete removes a chunk from the disk store.
// It removes the on-disk file, deletes the checksum index entry, and
// decrements usedSpace. Returns ChunkNotFound if the chunk does not exist.
func (ds *DiskStore) Delete(chunkId string) error {
	ds.logger.Debug("Delete", slog.String("chunkId", chunkId))

	if err := ds.validateChunkId(chunkId); err != nil {
		return err
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	if !ds.Exists(chunkId) {
		ds.logger.Debug("Delete: chunk not found", slog.String("chunkId", chunkId))
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
		ds.logger.Error("Delete: os.Remove failed", err, slog.String("path", chunkPath))
		return fmt.Errorf("%w: %v", FailedToDeleteChunk, err)
	}

	// Remove from checksum index; best-effort — if this fails we log
	// but don't return an error since the file is already gone.
	if err := ds.checksumStore.Delete(chunkId); err != nil && err != ErrKeyNotFound {
		ds.logger.Error("Delete: checksum index removal failed (non-fatal)", err,
			slog.String("chunkId", chunkId))
	}

	if ds.usedSpace >= fileSize {
		ds.usedSpace -= fileSize
	} else {
		ds.usedSpace = 0
	}

	ds.logger.Info("Delete: chunk removed",
		slog.String("chunkId", chunkId),
		slog.Uint64("freedBytes", fileSize),
		slog.Uint64("usedSpaceBytes", ds.usedSpace),
	)
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

// Write stores a value in the disk store.
// Although this func is not to be used often
// Since we use ChunkWriter to write our frames
// To add entry to DiskStore
// We Should Use DiskStore.Move/Rename
func (ds *DiskStore) Write(chunkId string, value []byte) error {
	ds.logger.Debug("Write", slog.String("chunkId", chunkId), slog.Int("bytes", len(value)))

	if err := ds.validateChunkId(chunkId); err != nil {
		return err
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Determine the existing file size so we can compute the net delta.
	// If the chunk already exists we subtract the old size before adding the new.
	var existingSize uint64
	if ds.Exists(chunkId) {
		relPath, err := ds.PathForChunk(chunkId)
		if err != nil {
			return err
		}
		info, err := os.Stat(filepath.Join(ds.rootDir, relPath))
		if err == nil {
			existingSize = uint64(info.Size())
		}
	}

	// Net space required: new bytes minus the bytes that will be freed.
	newSize := uint64(len(value))
	var netDelta uint64
	if newSize > existingSize {
		netDelta = newSize - existingSize
	}

	// Check if we have enough space for the *net* increase.
	freeSpace := ds.totalSpace - ds.usedSpace
	if netDelta > freeSpace {
		ds.logger.Error("Write: insufficient space", nil,
			slog.String("chunkId", chunkId),
			slog.Uint64("netNeeded", netDelta),
			slog.Uint64("available", freeSpace),
		)
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
		ds.logger.Error("Write: failed to create shard dir", err, slog.String("path", fullDirPath))
		return err
	}

	// Write the chunk file
	chunkPath := filepath.Join(fullDirPath, fmt.Sprintf("%s.%s", chunkId, "chunk"))
	if err := os.WriteFile(chunkPath, value, 0644); err != nil {
		ds.logger.Error("Write: os.WriteFile failed", err, slog.String("path", chunkPath))
		return err
	}

	// Calculate and store checksum
	checksum := sha256.Sum256(value)
	if err := ds.checksumStore.Put(chunkId, checksum[:]); err != nil {
		ds.logger.Error("Write: checksum put failed", err, slog.String("chunkId", chunkId))
		return err
	}

	// Update used space by the net delta (may be 0 or negative for shrinking overwrites).
	if newSize >= existingSize {
		ds.usedSpace += (newSize - existingSize)
	} else {
		ds.usedSpace -= (existingSize - newSize)
	}

	ds.logger.Info("Write: chunk stored",
		slog.String("chunkId", chunkId),
		slog.Int("bytes", len(value)),
		slog.Uint64("usedSpaceBytes", ds.usedSpace),
	)
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
	ds.logger.Debug("Rename", slog.String("source", source), slog.String("chunkId", chunkId))

	if err := ds.validateChunkId(chunkId); err != nil {
		return err
	}

	if _, err := os.Stat(source); err != nil {
		ds.logger.Error("Rename: source stat failed", err, slog.String("source", source))
		return err
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	relDest, err := ds.PathForChunk(chunkId)
	if err != nil {
		return err
	}
	dest := filepath.Join(ds.rootDir, relDest)
	destDir := filepath.Dir(dest)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		ds.logger.Error("Rename: failed to create dest dir", err, slog.String("destDir", destDir))
		return err
	}

	// Determine existing destination size (for overwrite delta accounting).
	var existingDestSize uint64
	if ds.Exists(chunkId) {
		if info, err := os.Stat(dest); err == nil {
			existingDestSize = uint64(info.Size())
		}
	}

	// Determine incoming source size for the free-space check.
	srcInfo, err := os.Stat(source)
	if err != nil {
		return err
	}
	srcSize := uint64(srcInfo.Size())

	// Net bytes we need: new size minus whatever the old chunk was using.
	var netDelta uint64
	if srcSize > existingDestSize {
		netDelta = srcSize - existingDestSize
	}

	freeSpace := ds.totalSpace - ds.usedSpace
	if netDelta > freeSpace {
		ds.logger.Error("Rename: insufficient space", nil,
			slog.String("chunkId", chunkId),
			slog.Uint64("netNeeded", netDelta),
			slog.Uint64("available", freeSpace),
		)
		return InsufficientSpace
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
		ds.logger.Error("Rename: os.Rename failed", err,
			slog.String("src", source), slog.String("dst", dest))
		return err
	}

	// fsync the destination directory so the rename is visible after crash.
	if dirFd, err := os.Open(destDir); err == nil {
		_ = dirFd.Sync()
		_ = dirFd.Close()
	}

	// Read the newly-placed file to compute its checksum.
	data, err := os.ReadFile(dest)
	if err != nil {
		return err
	}
	checksum := sha256.Sum256(data)
	if err := ds.checksumStore.Put(chunkId, checksum[:]); err != nil {
		ds.logger.Error("Rename: checksum put failed", err, slog.String("chunkId", chunkId))
		return err
	}

	// Adjust usedSpace by the net delta.
	if srcSize >= existingDestSize {
		ds.usedSpace += (srcSize - existingDestSize)
	} else {
		ds.usedSpace -= (existingDestSize - srcSize)
	}

	ds.logger.Info("Rename: chunk committed",
		slog.String("chunkId", chunkId),
		slog.String("dest", dest),
		slog.Uint64("bytes", srcSize),
		slog.Uint64("usedSpaceBytes", ds.usedSpace),
	)
	return nil
}

// Read retrieves a value from the disk store
func (ds *DiskStore) Read(chunkId string) ([]byte, error) {
	ds.logger.Debug("Read", slog.String("chunkId", chunkId))

	if err := ds.validateChunkId(chunkId); err != nil {
		return nil, ChunkNotFound
	}

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
		ds.logger.Error("Read: ReadFile failed", err, slog.String("path", chunkPath))
		return nil, err
	}

	ds.logger.Debug("Read: ok", slog.String("chunkId", chunkId), slog.Int("bytes", len(data)))
	return data, nil
}

// Verify checks the integrity of a key in the disk store
func (ds *DiskStore) Verify(chunkId string) error {
	ds.logger.Debug("Verify", slog.String("chunkId", chunkId))

	val, err := ds.checksumStore.Get(chunkId)
	if err != nil {
		return err
	}
	data, err := ds.Read(chunkId)
	if err != nil {
		return err
	}
	calculatedHash := sha256.Sum256(data)

	if !bytes.Equal(calculatedHash[:], val) {
		ds.logger.Error("Verify: checksum mismatch", dfserrors.ErrVerifyFailed,
			slog.String("chunkId", chunkId))
		return VerifyFailed
	}

	ds.logger.Debug("Verify: ok", slog.String("chunkId", chunkId))
	return nil
}

// TempDir returns the filepath for tempStoring of chunk.
// Returns InvalidChunkId if chunkId fails validation.
func (ds *DiskStore) TempDir(chunkId string) (string, error) {
	if err := ds.validateChunkId(chunkId); err != nil {
		return "", err
	}
	return filepath.Join(ds.tempDir, fmt.Sprintf("%s.%s", chunkId, ".tmp")), nil
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
	if ds.usedSpace > ds.totalSpace {
		return 0, nil
	}
	return ds.totalSpace - ds.usedSpace, nil
}

// Size returns the total size used by the store
func (ds *DiskStore) Size() (uint64, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
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
