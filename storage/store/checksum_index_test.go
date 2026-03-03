package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// tempDir creates a temporary directory for a test and registers a cleanup
// function that removes it after the test completes.
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "checksum_index_test_*")
	require.NoError(t, err, "failed to create temp dir")
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// newOpenedStore creates a BoltChecksumIndex[string] backed by a fresh temp
// directory, opens it, and registers CleanUp as a test cleanup handler.
func newOpenedStore(t *testing.T) *BoltChecksumIndex[string] {
	t.Helper()
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))
	require.NoError(t, store.Open(), "Open() failed")
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
		assert.NoErrorf(t, err, "Marshal(%q) unexpected error", tc)

		got, err := c.Unmarshal(b)
		assert.NoErrorf(t, err, "Unmarshal() unexpected error for input %q", tc)
		assert.Equalf(t, tc, got, "roundtrip mismatch for %q", tc)
	}
}

func TestStringCodec_Marshal_EmptyBytes(t *testing.T) {
	c := StringCodec{}
	b, err := c.Marshal("")
	require.NoError(t, err, "unexpected error marshalling empty string")
	assert.Empty(t, b, "expected empty bytes for empty string")
}

// ---------------------------------------------------------------------------
// Constructor / options tests
// ---------------------------------------------------------------------------

func TestNewChecksumIndexBoltDB_DefaultBucket(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](StringCodec{})
	assert.Equal(t, "checksums", store.bucket, "unexpected default bucket name")
}

func TestWithDbPath_SetsPath(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string]("/tmp/testpath"))
	assert.Equal(t, "/tmp/testpath", store.path)
}

func TestWithDefaultPath_SetsConventionalPath(t *testing.T) {
	want := filepath.Join(".", "storage", "store")

	store := NewChecksumIndexBoltDB[string](StringCodec{},
		WithDbPath[string]("/custom/path"),
		WithDefaultPath[string](),
	)
	assert.Equal(t, want, store.path, "WithDefaultPath should override a custom path")

	store2 := NewChecksumIndexBoltDB[string](StringCodec{})
	assert.Equal(t, want, store2.path, "default-constructed store should use conventional path")
}

func TestConstructor_SetsCodecFromPositionalArg(t *testing.T) {
	c := StringCodec{}
	store := NewChecksumIndexBoltDB[string](c)
	assert.NotNil(t, store.codec, "expected codec to be set via positional arg")
}

func TestNewChecksumIndexBoltDB_DatabaseNotOpenedByDefault(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](StringCodec{})
	assert.Nil(t, store.db, "expected db to be nil before Open()")
}

// ---------------------------------------------------------------------------
// Open tests
// ---------------------------------------------------------------------------

func TestOpen_CreatesDbFile(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))
	defer store.CleanUp()

	require.NoError(t, store.Open(), "Open() error")

	dbFile := filepath.Join(dir, "checksum.db")
	_, err := os.Stat(dbFile)
	assert.False(t, os.IsNotExist(err), "expected db file %q to exist after Open()", dbFile)
}

func TestOpen_CreatesBucket(t *testing.T) {
	store := newOpenedStore(t)
	// If the bucket was NOT created, Put would return "bucket not found".
	assert.NoError(t, store.Put("probe", "value"), "Put after Open() failed (bucket likely missing)")
}

func TestOpen_InvalidPath_ReturnsError(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](StringCodec{},
		WithDbPath[string]("/nonexistent/deeply/nested/path"),
	)
	defer store.CleanUp()

	assert.Error(t, store.Open(), "expected error when opening DB in non-existent directory")
}

func TestOpen_AfterCleanUp_CanReopenSameDir(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))

	require.NoError(t, store.Open(), "first Open() failed")
	store.CleanUp() // releases the file lock

	// A fresh instance should be able to open the same DB file.
	store2 := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))
	defer store2.CleanUp()
	assert.NoError(t, store2.Open(), "reopen failed")
}

func TestOpen_AlreadyOpen_ReturnsError(t *testing.T) {
	store := newOpenedStore(t)

	err := store.Open()
	assert.ErrorIs(t, err, ErrDatabaseAlreadyOpened)
}

// ---------------------------------------------------------------------------
// Put / Get tests
// ---------------------------------------------------------------------------

func TestPutAndGet_BasicRoundtrip(t *testing.T) {
	store := newOpenedStore(t)

	require.NoError(t, store.Put("key1", "value1"), "Put() error")

	got, err := store.Get("key1")
	require.NoError(t, err, "Get() error")
	assert.Equal(t, "value1", got)
}

func TestPut_OverwritesExistingKey(t *testing.T) {
	store := newOpenedStore(t)

	_ = store.Put("dup", "first")
	_ = store.Put("dup", "second")

	got, err := store.Get("dup")
	require.NoError(t, err, "Get() error")
	assert.Equal(t, "second", got, "expected overwritten value")
}

func TestGet_KeyNotFound_ReturnsErrKeyNotFound(t *testing.T) {
	store := newOpenedStore(t)

	_, err := store.Get("nonexistent")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestPut_BeforeOpen_ReturnsErrDatabaseNotOpened(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](StringCodec{})

	err := store.Put("k", "v")
	assert.ErrorIs(t, err, ErrDatabaseNotOpened)
}

func TestGet_BeforeOpen_ReturnsErrDatabaseNotOpened(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](StringCodec{})

	_, err := store.Get("k")
	assert.ErrorIs(t, err, ErrDatabaseNotOpened)
}

// TestPut_EmptyKey verifies that Put validates empty keys before reaching bbolt,
// returning the store's own ErrEmptyKey sentinel.
func TestPut_EmptyKey_ReturnsErrEmptyKey(t *testing.T) {
	store := newOpenedStore(t)

	err := store.Put("", "emptyKeyValue")
	assert.ErrorIs(t, err, ErrEmptyKey)
}

func TestPut_EmptyValue(t *testing.T) {
	store := newOpenedStore(t)

	require.NoError(t, store.Put("emptyval", ""), "Put with empty value failed")

	// bbolt stores empty values as nil slices; our Get will call Unmarshal("")
	// which for StringCodec is valid and returns "".
	got, err := store.Get("emptyval")
	require.NoError(t, err, "Get after Put of empty value failed")
	assert.Equal(t, "", got, "expected empty string value")
}

func TestPutAndGet_MultipleKeys(t *testing.T) {
	store := newOpenedStore(t)

	entries := map[string]string{
		"a": "alpha",
		"b": "beta",
		"c": "gamma",
	}
	for k, v := range entries {
		require.NoErrorf(t, store.Put(k, v), "Put(%q, %q) error", k, v)
	}
	for k, want := range entries {
		got, err := store.Get(k)
		require.NoErrorf(t, err, "Get(%q) error", k)
		assert.Equalf(t, want, got, "Get(%q) mismatch", k)
	}
}

func TestPutAndGet_LargeValue(t *testing.T) {
	store := newOpenedStore(t)

	largeVal := string(make([]byte, 1<<20)) // 1 MiB of zero bytes
	require.NoError(t, store.Put("large", largeVal), "Put large value failed")

	got, err := store.Get("large")
	require.NoError(t, err, "Get large value failed")
	assert.Equal(t, largeVal, got, "large value mismatch")
}

// ---------------------------------------------------------------------------
// CleanUp tests
// ---------------------------------------------------------------------------

func TestCleanUp_IdempotentOnNilDb(t *testing.T) {
	store := NewChecksumIndexBoltDB[string](StringCodec{})
	// Should not panic when db is nil.
	assert.NotPanics(t, store.CleanUp, "first CleanUp on nil db should not panic")
	assert.NotPanics(t, store.CleanUp, "second CleanUp on nil db should not panic")
}

func TestCleanUp_ClosesDatabase(t *testing.T) {
	dir := tempDir(t)
	store := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))
	require.NoError(t, store.Open(), "Open() failed")

	store.CleanUp()

	// After CleanUp, c.db is set to nil, so the nil-guard in Put returns
	// ErrDatabaseNotOpened — no bbolt involvement needed.
	err := store.Put("post-close", "v")
	assert.ErrorIs(t, err, ErrDatabaseNotOpened, "expected ErrDatabaseNotOpened after CleanUp")
}

func TestCleanUp_SetsDbToNil(t *testing.T) {
	store := newOpenedStore(t)
	store.CleanUp() // registered cleanup will call it again safely
	assert.Nil(t, store.db, "expected store.db to be nil after CleanUp")
}

// ---------------------------------------------------------------------------
// Persistence tests
// ---------------------------------------------------------------------------

func TestPersistence_DataSurvivesReopen(t *testing.T) {
	dir := tempDir(t)

	// Write data with the first instance.
	s1 := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))
	require.NoError(t, s1.Open(), "Open() failed")
	require.NoError(t, s1.Put("persistent", "data"), "Put() failed")
	s1.CleanUp()

	// Read data with a fresh instance pointing at the same directory.
	s2 := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))
	require.NoError(t, s2.Open(), "second Open() failed")
	defer s2.CleanUp()

	got, err := s2.Get("persistent")
	require.NoError(t, err, "Get() after reopen failed")
	assert.Equal(t, "data", got)
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
			assert.NoErrorf(t, store.Put(key, val), "concurrent Put(%q) error", key)
		}(i)
	}
	wg.Wait()

	// Verify all writes landed correctly.
	for i := range goroutines {
		key := fmt.Sprintf("key-%d", i)
		want := fmt.Sprintf("val-%d", i)
		got, err := store.Get(key)
		if assert.NoErrorf(t, err, "Get(%q) error", key) {
			assert.Equalf(t, want, got, "Get(%q) mismatch", key)
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
	store := NewChecksumIndexBoltDB[string](errCodec{}, WithDbPath[string](dir))
	require.NoError(t, store.Open(), "Open() failed")
	defer store.CleanUp()

	assert.Error(t, store.Put("k", "v"), "expected marshal error, got nil")
}

func TestGet_UnmarshalError_ReturnsError(t *testing.T) {
	dir := tempDir(t)

	// Write valid data via a working codec.
	goodStore := NewChecksumIndexBoltDB[string](StringCodec{}, WithDbPath[string](dir))
	require.NoError(t, goodStore.Open(), "goodStore Open() failed")
	require.NoError(t, goodStore.Put("k", "v"), "goodStore Put() failed")
	goodStore.CleanUp()

	// Now read with the errCodec — Unmarshal should fail.
	badStore := NewChecksumIndexBoltDB[string](errCodec{}, WithDbPath[string](dir))
	require.NoError(t, badStore.Open(), "badStore Open() failed")
	defer badStore.CleanUp()

	_, err := badStore.Get("k")
	assert.Error(t, err, "expected unmarshal error, got nil")
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
	store := NewChecksumIndexBoltDB[string](StringCodec{})

	// ErrDatabaseNotOpened
	err := store.Put("k", "v")
	assert.ErrorIs(t, err, ErrDatabaseNotOpened, "Put before Open: expected ErrDatabaseNotOpened")

	_, err = store.Get("k")
	assert.ErrorIs(t, err, ErrDatabaseNotOpened, "Get before Open: expected ErrDatabaseNotOpened")

	// ErrDatabaseAlreadyOpened
	opened := newOpenedStore(t)
	assert.ErrorIs(t, opened.Open(), ErrDatabaseAlreadyOpened, "double Open: expected ErrDatabaseAlreadyOpened")

	// ErrEmptyKey
	assert.ErrorIs(t, opened.Put("", "v"), ErrEmptyKey, "empty key Put: expected ErrEmptyKey")

	// ErrKeyNotFound
	_, err = opened.Get("__no_such_key__")
	assert.ErrorIs(t, err, ErrKeyNotFound, "missing key Get: expected ErrKeyNotFound")
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
	store := NewChecksumIndexBoltDB[int](intCodec{}, WithDbPath[int](dir))
	require.NoError(t, store.Open(), "Open() failed")
	defer store.CleanUp()

	require.NoError(t, store.Put("count", 42), "Put() failed")

	got, err := store.Get("count")
	require.NoError(t, err, "Get() failed")
	assert.Equal(t, 42, got)
}

func TestGetAllKeys(t *testing.T) {
	type testCase struct {
		name           string
		keys           []KeyValue[string]
		expectedResult []string
		expectedError  error
	}

	testCases := []testCase{
		{
			name: "valid test case with multiple keys",
			keys: []KeyValue[string]{
				{key: "first", value: "value1"},
				{key: "sec", value: "value2"},
				{key: "third", value: "value3"},
			},
			expectedResult: []string{"first", "sec", "third"},
			expectedError:  nil,
		},
		{
			name:           "empty store",
			keys:           []KeyValue[string]{},
			expectedResult: []string{},
			expectedError:  nil,
		},
		{
			name: "single key",
			keys: []KeyValue[string]{
				{key: "only", value: "value"},
			},
			expectedResult: []string{"only"},
			expectedError:  nil,
		},
		{
			name:           "empty bucket",
			keys:           []KeyValue[string]{},
			expectedResult: []string{},
			expectedError:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newOpenedStore(t)

			// Populate store with test data
			assert.NoError(t, store.PutAll(tc.keys), "PutAll failed")

			// Call GetAllKeys (assuming this method exists)
			got, err := store.GetAll()

			if tc.expectedError != nil {
				assert.ErrorIs(t, err, tc.expectedError)
			} else {
				assert.NoError(t, err, "GetAllKeys() failed")
				assert.ElementsMatch(t, tc.expectedResult, got, "keys mismatch")
			}
		})
	}
}
