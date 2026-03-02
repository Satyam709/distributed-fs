package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// tempDir creates a temporary directory for a test and registers a cleanup
// function that removes it after the test completes.
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "checksum_index_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// newOpenedStore creates a BoltChecksumIndex[string] backed by a fresh temp
// directory, opens it, and registers CleanUp as a test cleanup handler.
func newOpenedStore(t *testing.T) *BoltChecksumIndex[string] {
	t.Helper()
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))
	if err := store.Open(); err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	t.Cleanup(store.CleanUp)
	return store
}

// ---------------------------------------------------------------------------
// Codec tests
// ---------------------------------------------------------------------------

func TestStringCodec_Marshal_Roundtrip(t *testing.T) {
	c := StringCodec{}
	cases := []string{"", "hello", "abc\x00def", "日本語"}
	for _, tc := range cases {
		b, err := c.Marshal(tc)
		if err != nil {
			t.Errorf("Marshal(%q) unexpected error: %v", tc, err)
		}
		got, err := c.Unmarshal(b)
		if err != nil {
			t.Errorf("Unmarshal() unexpected error: %v", err)
		}
		if got != tc {
			t.Errorf("roundtrip: got %q, want %q", got, tc)
		}
	}
}

func TestStringCodec_Marshal_EmptyBytes(t *testing.T) {
	c := StringCodec{}
	b, err := c.Marshal("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) != 0 {
		t.Errorf("expected empty bytes for empty string, got len=%d", len(b))
	}
}

// ---------------------------------------------------------------------------
// Constructor / options tests
// ---------------------------------------------------------------------------

func TestNewChecksumIndexBoltDB_DefaultBucket(t *testing.T) {
	store := NewChecksumIndexBoltDB[string]()
	if store.bucket != "checksums" {
		t.Errorf("expected default bucket 'checksums', got %q", store.bucket)
	}
}

func TestWithDbPath_SetsPath(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](WithDbPath[string]("/tmp/testpath"))
	if store.path != "/tmp/testpath" {
		t.Errorf("expected path '/tmp/testpath', got %q", store.path)
	}
}

func TestWithDefaultPath_SetsConventionalPath(t *testing.T) {
	// WithDefaultPath takes no argument and always sets ./storage/store.
	store := NewChecksumIndexBoltDB[string](
		WithDbPath[string]("/custom/path"),
		WithDefaultPath[string](),
	)
	want := filepath.Join(".", "storage", "store")
	if store.path != want {
		t.Errorf("expected path %q, got %q", want, store.path)
	}
}

func TestWithCodec_SetsCodec(t *testing.T) {
	c := StringCodec{}
	store := NewChecksumIndexBoltDB[string](WithCodec(c))
	if store.codec == nil {
		t.Error("expected codec to be set, got nil")
	}
}

func TestNewChecksumIndexBoltDB_DatabaseNotOpenedByDefault(t *testing.T) {
	store := NewChecksumIndexBoltDB[string]()
	if store.db != nil {
		t.Error("expected db to be nil before Open()")
	}
}

// ---------------------------------------------------------------------------
// Open tests
// ---------------------------------------------------------------------------

func TestOpen_CreatesDbFile(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))
	defer store.CleanUp()

	if err := store.Open(); err != nil {
		t.Fatalf("Open() error: %v", err)
	}

	dbFile := filepath.Join(dir, "checksum.db")
	if _, err := os.Stat(dbFile); os.IsNotExist(err) {
		t.Errorf("expected db file %q to exist after Open()", dbFile)
	}
}

func TestOpen_CreatesBucket(t *testing.T) {
	store := newOpenedStore(t)
	// If the bucket was NOT created, Put would return "bucket not found".
	if err := store.Put("probe", "value"); err != nil {
		t.Errorf("Put after Open() failed (bucket likely missing): %v", err)
	}
}

func TestOpen_InvalidPath_ReturnsError(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](
		WithDbPath[string]("/nonexistent/deeply/nested/path"),
		WithCodec(StringCodec{}),
	)
	defer store.CleanUp()

	if err := store.Open(); err == nil {
		t.Error("expected error when opening DB in non-existent directory, got nil")
	}
}

func TestOpen_AfterCleanUp_CanReopenSameDir(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))

	if err := store.Open(); err != nil {
		t.Fatalf("first Open() failed: %v", err)
	}
	store.CleanUp() // releases the file lock

	// A fresh instance should be able to open the same DB file.
	store2 := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))
	defer store2.CleanUp()
	if err := store2.Open(); err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
}

func TestOpen_AlreadyOpen_ReturnsError(t *testing.T) {
	store := newOpenedStore(t)

	err := store.Open()
	if !errors.Is(err, ErrDatabaseAlreadyOpened) {
		t.Errorf("expected ErrDatabaseAlreadyOpened, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Put / Get tests
// ---------------------------------------------------------------------------

func TestPutAndGet_BasicRoundtrip(t *testing.T) {
	store := newOpenedStore(t)

	if err := store.Put("key1", "value1"); err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	got, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got != "value1" {
		t.Errorf("Get() = %q, want %q", got, "value1")
	}
}

func TestPut_OverwritesExistingKey(t *testing.T) {
	store := newOpenedStore(t)

	_ = store.Put("dup", "first")
	_ = store.Put("dup", "second")

	got, err := store.Get("dup")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got != "second" {
		t.Errorf("expected overwritten value 'second', got %q", got)
	}
}

func TestGet_KeyNotFound_ReturnsErrKeyNotFound(t *testing.T) {
	store := newOpenedStore(t)

	_, err := store.Get("nonexistent")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestPut_BeforeOpen_ReturnsErrDatabaseNotOpened(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](WithCodec(StringCodec{}))

	err := store.Put("k", "v")
	if !errors.Is(err, ErrDatabaseNotOpened) {
		t.Errorf("expected ErrDatabaseNotOpened, got %v", err)
	}
}

func TestGet_BeforeOpen_ReturnsErrDatabaseNotOpened(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](WithCodec(StringCodec{}))

	_, err := store.Get("k")
	if !errors.Is(err, ErrDatabaseNotOpened) {
		t.Errorf("expected ErrDatabaseNotOpened, got %v", err)
	}
}

// TestPut_EmptyKey verifies that Put validates empty keys before reaching bbolt,
// returning the store's own ErrEmptyKey sentinel.
func TestPut_EmptyKey_ReturnsErrEmptyKey(t *testing.T) {
	store := newOpenedStore(t)

	err := store.Put("", "emptyKeyValue")
	if !errors.Is(err, ErrEmptyKey) {
		t.Errorf("expected ErrEmptyKey, got %v", err)
	}
}

func TestPut_EmptyValue(t *testing.T) {
	store := newOpenedStore(t)

	if err := store.Put("emptyval", ""); err != nil {
		t.Fatalf("Put with empty value failed: %v", err)
	}

	// bbolt stores empty values as nil slices; our Get will call Unmarshal("")
	// which for StringCodec is valid and returns "".
	got, err := store.Get("emptyval")
	if err != nil {
		t.Fatalf("Get after Put of empty value failed: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestPutAndGet_MultipleKeys(t *testing.T) {
	store := newOpenedStore(t)

	entries := map[string]string{
		"a": "alpha",
		"b": "beta",
		"c": "gamma",
	}
	for k, v := range entries {
		if err := store.Put(k, v); err != nil {
			t.Fatalf("Put(%q, %q) error: %v", k, v, err)
		}
	}
	for k, want := range entries {
		got, err := store.Get(k)
		if err != nil {
			t.Fatalf("Get(%q) error: %v", k, err)
		}
		if got != want {
			t.Errorf("Get(%q) = %q, want %q", k, got, want)
		}
	}
}

func TestPutAndGet_LargeValue(t *testing.T) {
	store := newOpenedStore(t)

	largeVal := string(make([]byte, 1<<20)) // 1 MiB of zero bytes
	if err := store.Put("large", largeVal); err != nil {
		t.Fatalf("Put large value failed: %v", err)
	}
	got, err := store.Get("large")
	if err != nil {
		t.Fatalf("Get large value failed: %v", err)
	}
	if got != largeVal {
		t.Error("large value mismatch")
	}
}

// ---------------------------------------------------------------------------
// CleanUp tests
// ---------------------------------------------------------------------------

func TestCleanUp_IdempotentOnNilDb(t *testing.T) {
	store := NewChecksumIndexBoltDB[string]()
	// Should not panic when db is nil.
	store.CleanUp()
	store.CleanUp()
}

func TestCleanUp_ClosesDatabase(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))
	if err := store.Open(); err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	store.CleanUp()

	// After CleanUp, c.db is set to nil, so the nil-guard in Put returns
	// ErrDatabaseNotOpened — no bbolt involvement needed.
	err := store.Put("post-close", "v")
	if !errors.Is(err, ErrDatabaseNotOpened) {
		t.Errorf("expected ErrDatabaseNotOpened after CleanUp, got %v", err)
	}
}

func TestCleanUp_SetsDbToNil(t *testing.T) {
	store := newOpenedStore(t)
	store.CleanUp() // registered cleanup will call it again safely
	if store.db != nil {
		t.Error("expected store.db to be nil after CleanUp")
	}
}

// ---------------------------------------------------------------------------
// Persistence tests
// ---------------------------------------------------------------------------

func TestPersistence_DataSurvivesReopen(t *testing.T) {
	dir := tempDir(t)

	// Write data with the first instance.
	s1 := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))
	if err := s1.Open(); err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	if err := s1.Put("persistent", "data"); err != nil {
		t.Fatalf("Put() failed: %v", err)
	}
	s1.CleanUp()

	// Read data with a fresh instance pointing at the same directory.
	s2 := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))
	if err := s2.Open(); err != nil {
		t.Fatalf("second Open() failed: %v", err)
	}
	defer s2.CleanUp()

	got, err := s2.Get("persistent")
	if err != nil {
		t.Fatalf("Get() after reopen failed: %v", err)
	}
	if got != "data" {
		t.Errorf("expected 'data', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Concurrency tests
// ---------------------------------------------------------------------------

func TestConcurrentPuts_NoDataRaces(t *testing.T) {
	store := newOpenedStore(t)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", n)
			val := fmt.Sprintf("val-%d", n)
			if err := store.Put(key, val); err != nil {
				t.Errorf("concurrent Put(%q) error: %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all writes landed correctly.
	for i := range goroutines {
		key := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("val-%d", i)
		got, err := store.Get(key)
		if err != nil {
			t.Errorf("Get(%q) error: %v", key, err)
			continue
		}
		if got != want {
			t.Errorf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestConcurrentPutAndGet_NoDataRaces(t *testing.T) {
	store := newOpenedStore(t)

	// Pre-populate a key that readers race against writers.
	_ = store.Put("shared", "initial")

	var wg sync.WaitGroup
	for range 30 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = store.Put("shared", "updated")
		}()
		go func() {
			defer wg.Done()
			_, _ = store.Get("shared")
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// Custom codec tests
// ---------------------------------------------------------------------------

// errCodec is a Codec that always returns errors — used to exercise error paths.
type errCodec struct{}

func (errCodec) Marshal(_ string) ([]byte, error) {
	return nil, errors.New("marshal error")
}
func (errCodec) Unmarshal(_ []byte) (string, error) {
	return "", errors.New("unmarshal error")
}

func TestPut_MarshalError_ReturnsError(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(errCodec{}))
	if err := store.Open(); err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer store.CleanUp()

	if err := store.Put("k", "v"); err == nil {
		t.Error("expected marshal error, got nil")
	}
}

func TestGet_UnmarshalError_ReturnsError(t *testing.T) {
	dir := tempDir(t)

	// Write valid data via a working codec.
	goodStore := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(StringCodec{}))
	if err := goodStore.Open(); err != nil {
		t.Fatalf("goodStore Open() failed: %v", err)
	}
	if err := goodStore.Put("k", "v"); err != nil {
		t.Fatalf("goodStore Put() failed: %v", err)
	}
	goodStore.CleanUp()

	// Now read with the errCodec — Unmarshal should fail.
	badStore := NewChecksumIndexBoltDB[string](WithDbPath[string](dir), WithCodec(errCodec{}))
	if err := badStore.Open(); err != nil {
		t.Fatalf("badStore Open() failed: %v", err)
	}
	defer badStore.CleanUp()

	if _, err := badStore.Get("k"); err == nil {
		t.Error("expected unmarshal error, got nil")
	}
}

// ---------------------------------------------------------------------------
// Interface compliance
// ---------------------------------------------------------------------------

func TestBoltChecksumIndex_ImplementsInterface(t *testing.T) {
	// Static compile-time assertion — if this compiles the interface is satisfied.
	var _ ChecksumIndexStore[string] = (*BoltChecksumIndex[string])(nil)
}

// TestSentinelErrors_AreInspectable verifies that all sentinel errors defined
// by the package can be matched with errors.Is, i.e. they are not wrapped in
// a way that loses identity.
func TestSentinelErrors_AreInspectable(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](WithCodec(StringCodec{}))

	// ErrDatabaseNotOpened
	if err := store.Put("k", "v"); !errors.Is(err, ErrDatabaseNotOpened) {
		t.Errorf("Put before Open: expected ErrDatabaseNotOpened, got %v", err)
	}
	if _, err := store.Get("k"); !errors.Is(err, ErrDatabaseNotOpened) {
		t.Errorf("Get before Open: expected ErrDatabaseNotOpened, got %v", err)
	}

	// ErrDatabaseAlreadyOpened
	opened := newOpenedStore(t)
	if err := opened.Open(); !errors.Is(err, ErrDatabaseAlreadyOpened) {
		t.Errorf("double Open: expected ErrDatabaseAlreadyOpened, got %v", err)
	}

	// ErrEmptyKey
	if err := opened.Put("", "v"); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("empty key Put: expected ErrEmptyKey, got %v", err)
	}

	// ErrKeyNotFound
	if _, err := opened.Get("__no_such_key__"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("missing key Get: expected ErrKeyNotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Generic type test
// ---------------------------------------------------------------------------

// intCodec encodes an int as a little-endian 8-byte slice.
type intCodec struct{}

func (intCodec) Marshal(v int) ([]byte, error) {
	b := make([]byte, 8)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	return b, nil
}

func (intCodec) Unmarshal(b []byte) (int, error) {
	if len(b) < 4 {
		return 0, fmt.Errorf("too short: %d bytes", len(b))
	}
	return int(b[0]) | int(b[1])<<8 | int(b[2])<<16 | int(b[3])<<24, nil
}

func TestBoltChecksumIndex_IntGenericType(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[int](WithDbPath[int](dir), WithCodec(intCodec{}))
	if err := store.Open(); err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer store.CleanUp()

	if err := store.Put("count", 42); err != nil {
		t.Fatalf("Put() failed: %v", err)
	}
	got, err := store.Get("count")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}
