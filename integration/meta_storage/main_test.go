//go:build integration

package meta_storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/satyam709/distributed-fs/integration/testutil"
)

var tc *testutil.TestCluster

func TestMain(m *testing.M) {
	t := &testingTShim{}
	tc = testutil.StartTestCluster(t, 3) // 1 metadata + 3 storage nodes
	if t.failed {
		fmt.Fprintf(os.Stderr, "cluster startup failed\n")
		os.Exit(1)
	}
	code := m.Run()
	tc.Shutdown()
	os.Exit(code)
}

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
	dir, err := os.MkdirTemp("", "dfs-meta-storage-*")
	if err != nil {
		s.Fatalf("TempDir: %v", err)
	}
	s.cleanups = append(s.cleanups, func() { _ = os.RemoveAll(dir) })
	return dir
}
func (s *testingTShim) Cleanup(f func()) { s.cleanups = append(s.cleanups, f) }
