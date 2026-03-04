package store

type Store interface {
	Write(string, []byte) error
	Rename(sourcePath string, chunkId string) error
	Read(string) ([]byte, error)
	Delete(string) error
	Exists(string) bool
	TempDir(string) (string, error)
	Verify(string) error
	List() ([]string, error)
	FreeSpace() (uint64, error)
	PathForChunk(string) (string, error)
	Size() (uint64, error)
}

// ChecksumIndexStore is a generic persistent key→value store that maps
// string keys to values of type T.
//
// Implementations must be opened with Open before use and released
// with CleanUp when done.
type ChecksumIndexStore[T any] interface {
	// Open initialises the underlying storage (e.g. opens a DB file).
	Open() error
	// Put writes value under key, overwriting any existing entry.
	Put(string, T) error
	// Get retrieves the value stored under key.
	Get(string) (T, error)
	// GetAll retrieves all the stored keys.
	GetAll() ([]string, error)
	// Delete removes the entry for key. Returns ErrKeyNotFound if the key does
	// not exist and ErrDatabaseNotOpened if Open has not been called.
	Delete(string) error
	// PutAll puts all the keys at once
	PutAll([]KeyValue[T]) error
	// CleanUp releases all resources held by the store.
	CleanUp()
}
type KeyValue[T any] struct {
	Key   string
	Value T
}
