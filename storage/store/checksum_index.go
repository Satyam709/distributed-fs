package store

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/satyam709/distributed-fs/internal/logging"
	"go.etcd.io/bbolt"
)

// errors
var (
	ErrDatabaseNotOpened     = errors.New("database not opened")
	ErrDatabaseAlreadyOpened = errors.New("database already opened")
	ErrKeyNotFound           = errors.New("key not found")
	ErrBucketNotFound        = errors.New("bucket not found")
	ErrEmptyKey              = errors.New("key must not be empty")
)

// Codec defines how values of type T are serialised to and from bytes.
//
// Contract: Marshal must never return a nil byte slice; use []byte{} for
// zero/empty values. A nil return from Marshal is indistinguishable from a
// missing key when reading from bbolt.
type Codec[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
}

// StringCodec is a Codec[string] that converts strings to/from raw bytes.
type StringCodec struct{}

func (StringCodec) Marshal(v string) ([]byte, error) {
	return []byte(v), nil
}

func (StringCodec) Unmarshal(b []byte) (string, error) {
	return string(b), nil
}

// BoltChecksumIndex is a generic key→value store backed by bbolt.
type BoltChecksumIndex[T any] struct {
	db     *bbolt.DB
	path   string
	logger *logging.CLogger
	bucket string
	codec  Codec[T]
}

// compile-time assertion that *BoltChecksumIndex[string] satisfies the interface.
var _ ChecksumIndexStore[string] = (*BoltChecksumIndex[string])(nil)

type BoltChecksumIndexOpts[T any] func(*BoltChecksumIndex[T])

// WithDbPath sets the directory in which checksum.db is created.
func WithDbPath[T any](path string) BoltChecksumIndexOpts[T] {
	return func(b *BoltChecksumIndex[T]) {
		b.path = path
	}
}

// WithCodec sets the codec used to marshal/unmarshal values.
func WithCodec[T any](codec Codec[T]) BoltChecksumIndexOpts[T] {
	return func(b *BoltChecksumIndex[T]) {
		b.codec = codec
	}
}

// WithDefaultPath sets the store path to the module-conventional default
// (./storage/store). The path argument is intentionally absent: if you need
// a custom path use WithDbPath instead.
func WithDefaultPath[T any]() BoltChecksumIndexOpts[T] {
	return func(b *BoltChecksumIndex[T]) {
		b.path = filepath.Join(".", "storage", "store")
	}
}

// NewChecksumIndexBoltDB constructs a BoltChecksumIndex with the given options.
// Call Open before using Put or Get.
func NewChecksumIndexBoltDB[T any](opts ...BoltChecksumIndexOpts[T]) *BoltChecksumIndex[T] {
	boltStore := &BoltChecksumIndex[T]{
		logger: logging.NewCLogger(),
		bucket: "checksums",
	}

	WithDefaultPath[T]()(boltStore)

	boltStore.logger.Logger = *boltStore.logger.Logger.With(slog.String("component", "BoltChecksumIndex"))

	for _, opt := range opts {
		opt(boltStore)
	}
	return boltStore
}

// Open opens (or creates) the bbolt database file and ensures the bucket
// exists. Returns ErrDatabaseAlreadyOpened if Open has already been called.
func (c *BoltChecksumIndex[T]) Open() error {
	if c.db != nil {
		return ErrDatabaseAlreadyOpened
	}

	dbFile, err := filepath.Abs(filepath.Join(c.path, "checksum.db"))

	if err != nil {
		return err
	}

	// ensure the path exist
	if err = os.MkdirAll(filepath.Dir(dbFile), 0700); err != nil {
		return err
	}

	c.db, err = bbolt.Open(dbFile, 0600, nil)
	if err != nil {
		return err
	}
	return c.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(c.bucket))
		return err
	})
}

// Put stores value under key. Returns ErrEmptyKey for empty keys and
// ErrDatabaseNotOpened if Open has not been called.
func (c *BoltChecksumIndex[T]) Put(key string, value T) error {
	if key == "" {
		return ErrEmptyKey
	}
	if c.db == nil {
		return ErrDatabaseNotOpened
	}

	return c.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucket))
		if bucket == nil {
			return ErrBucketNotFound
		}

		data, err := c.codec.Marshal(value)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), data)
	})
}

// Get retrieves the value stored under key. Returns ErrKeyNotFound when the
// key does not exist and ErrDatabaseNotOpened if Open has not been called.
func (c *BoltChecksumIndex[T]) Get(key string) (T, error) {
	var zero T

	if c.db == nil {
		return zero, ErrDatabaseNotOpened
	}

	var result T

	err := c.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucket))
		if bucket == nil {
			return ErrBucketNotFound
		}

		data := bucket.Get([]byte(key))
		if data == nil {
			return ErrKeyNotFound
		}

		decoded, err := c.codec.Unmarshal(data)
		if err != nil {
			return err
		}

		result = decoded
		return nil
	})

	return result, err
}

// CleanUp closes the underlying bbolt database. It is safe to call multiple
// times; subsequent calls are no-ops.
func (c *BoltChecksumIndex[T]) CleanUp() {
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			c.logger.Error("Error while closing", err)
		}
		c.db = nil
	}
}
