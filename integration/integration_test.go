//go:build integration

package integration

import (
	"fmt"
	"os"
	"testing"
)

// testCluster is a shared mini-cluster that is booted once for the entire
// test suite and torn down after all tests finish. Individual tests use
// unique file/chunk IDs to avoid state collisions.
var testCluster *TestCluster

func TestMain(m *testing.M) {
	// Use a lightweight testing.T-like wrapper for the cluster setup.
	// TestMain doesn't have a *testing.T, so we use a shim.
	t := &testingTShim{}
	testCluster = StartTestCluster(t, 2) // 1 metadata + 2 storage nodes

	if t.failed {
		fmt.Fprintf(os.Stderr, "cluster startup failed\n")
		os.Exit(1)
	}

	code := m.Run()

	testCluster.Shutdown()
	os.Exit(code)
}

// testingTShim satisfies the subset of testing.TB used by StartTestCluster
// when called from TestMain (which has no real *testing.T).
type testingTShim struct {
	failed   bool
	cleanups []func()
}

func (s *testingTShim) Helper()                         {}
func (s *testingTShim) Logf(format string, args ...any) { fmt.Printf(format+"\n", args...) }
func (s *testingTShim) Fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	s.failed = true
}
func (s *testingTShim) TempDir() string {
	dir, err := os.MkdirTemp("", "dfs-integration-*")
	if err != nil {
		s.Fatalf("TempDir: %v", err)
	}
	s.cleanups = append(s.cleanups, func() { os.RemoveAll(dir) })
	return dir
}
func (s *testingTShim) Cleanup(f func()) { s.cleanups = append(s.cleanups, f) }
