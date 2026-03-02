package store

type Store interface {
	Write(key string, value any) error
	Read(key string) (any, error)
	Delete(key string) error
	Exists(key string) bool
	Verify(key string) error
	List() ([]string, error)
	FreeSpace() (int64, error)
	Size() (int64, error)
}

// ChecksumIndexStore is a generic persistent key→value store that maps
// string keys to values of type T.
//
// Implementations must be opened with Open before use and released with
// CleanUp when done.
type ChecksumIndexStore[T any] interface {
	// Open initialises the underlying storage (e.g. opens a DB file).
	Open() error
	// Put writes value under key, overwriting any existing entry.
	Put(key string, value T) error
	// Get retrieves the value stored under key.
	Get(key string) (T, error)
	// CleanUp releases all resources held by the store.
	CleanUp()
}
