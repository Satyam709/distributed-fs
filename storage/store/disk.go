package store

type DiskStore struct {
	// rootDir is the base directory path where all chunks are stored
	rootDir string
	// splitLevel determines the depth of directory sharding for chunk storage.
	// For example, with splitLevel=2, a chunk named "adwjij2jj424" is stored at
	// rootDir/ad/wj/adwjij2jj424.chunk, distributing chunks across subdirectories
	// to avoid filesystem performance issues with too many files in a single directory
	splitLevel int16
}

// DiskStoreBuilder
type DiskStoreOptions func(*DiskStore) 

var (
	// a compile-time check to ensure we implement the store interface correctly
	_ Store = (*DiskStore)(nil)
)

// Delete removes a value from the disk store
func (ds *DiskStore) Delete(chunkId string) error {
	return nil
}

// List returns all keys in the disk store
func (ds *DiskStore) List() ([]string, error) {
	return nil, nil
}

// Exists checks if a key exists in the disk store
func (ds *DiskStore) Exists(chunkId string) bool {
	return false
}

// Write stores a value in the disk store
func (ds *DiskStore) Write(chunkId string, value any) error {
	return nil
}

// Read retrieves a value from the disk store
func (ds *DiskStore) Read(key string) (any, error) {
	return nil, nil
}

// Verify checks the integrity of a key in the disk store
func (ds *DiskStore) Verify(key string) error {
	return nil
}

// FreeSpace returns the available space on disk
func (ds *DiskStore) FreeSpace() (int64, error) {
	return 0, nil
}

// Size returns the total size used by the store
func (ds *DiskStore) Size() (int64, error) {
	return 0, nil
}
