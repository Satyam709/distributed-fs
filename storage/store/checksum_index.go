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

// []ByteCodec is a Codec[[]byte] its a stub as by-default bblot stores vals in []byte
type ByteCodec struct{}

func (ByteCodec) Marshal(v []byte) ([]byte, error) {
	return v, nil
}

func (ByteCodec) Unmarshal(b []byte) ([]byte, error) {
	return b, nil
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
		c.logger.Debug("Open: database already open")
		return ErrDatabaseAlreadyOpened
	}

	dbFile, err := filepath.Abs(filepath.Join(c.path, "checksum.db"))
	if err != nil {
		c.logger.Error("Open: failed to resolve db path", err, slog.String("path", c.path))
		return err
	}

	c.logger.Info("Open: opening BoltDB", slog.String("file", dbFile))

	// ensure the path exists
	if err = os.MkdirAll(filepath.Dir(dbFile), 0700); err != nil {
		c.logger.Error("Open: failed to create db dir", err, slog.String("dir", filepath.Dir(dbFile)))
		return err
	}

	c.db, err = bbolt.Open(dbFile, 0600, nil)
	if err != nil {
		c.logger.Error("Open: bbolt.Open failed", err, slog.String("file", dbFile))
		return err
	}

	err = c.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(c.bucket))
		return err
	})
	if err != nil {
		c.logger.Error("Open: failed to create bucket", err, slog.String("bucket", c.bucket))
		return err
	}

	c.logger.Info("Open: ready", slog.String("file", dbFile), slog.String("bucket", c.bucket))
	return nil
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

	c.logger.Debug("Put", slog.String("key", key))

	err := c.db.Update(func(tx *bbolt.Tx) error {
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
	if err != nil {
		c.logger.Error("Put: failed", err, slog.String("key", key))
	}
	return err
}

// Get retrieves the value stored under key. Returns ErrKeyNotFound when the
// key does not exist and ErrDatabaseNotOpened if Open has not been called.
func (c *BoltChecksumIndex[T]) Get(key string) (T, error) {
	var zero T

	if c.db == nil {
		return zero, ErrDatabaseNotOpened
	}

	c.logger.Debug("Get", slog.String("key", key))

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

	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		c.logger.Error("Get: failed", err, slog.String("key", key))
	}
	return result, err
}

func (c *BoltChecksumIndex[T]) GetAll() ([]string, error) {
	var zero []string

	if c.db == nil {
		return zero, ErrDatabaseNotOpened
	}

	result := make([]string, 0)

	err := c.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucket))
		if bucket == nil {
			return ErrBucketNotFound
		}

		err := bucket.ForEach(func(k, v []byte) error {
			result = append(result, string(k))
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})

	return result, err
}

func (c *BoltChecksumIndex[T]) PutAll(data []KeyValue[T]) error {
	if c.db == nil {
		return ErrDatabaseNotOpened
	}

	for _, val := range data {
		err := c.Put(val.key, val.value)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete removes the entry stored under key. Returns ErrEmptyKey for empty
// keys, ErrDatabaseNotOpened if Open has not been called, and ErrKeyNotFound
// if the key does not exist in the bucket.
func (c *BoltChecksumIndex[T]) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	if c.db == nil {
		return ErrDatabaseNotOpened
	}

	c.logger.Debug("Delete", slog.String("key", key))

	err := c.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(c.bucket))
		if bucket == nil {
			return ErrBucketNotFound
		}
		if bucket.Get([]byte(key)) == nil {
			return ErrKeyNotFound
		}
		return bucket.Delete([]byte(key))
	})
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		c.logger.Error("Delete: failed", err, slog.String("key", key))
	}
	return err
}

// CleanUp closes the underlying bbolt database. It is safe to call multiple
// times; subsequent calls are no-ops.
func (c *BoltChecksumIndex[T]) CleanUp() {
	if c.db == nil {
		return
	}
	c.logger.Info("CleanUp: closing BoltDB")
	if err := c.db.Close(); err != nil {
		c.logger.Error("CleanUp: error closing database", err)
	}
	c.db = nil
	c.logger.Info("CleanUp: database closed")
}
