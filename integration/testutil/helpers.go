// Package testutil provides shared types and constructors for integration
// tests across the integration/ hierarchy. It is a regular (non-test) package
// so that sub-packages like metastore/, failover/, and e2e/ can import it.
//
// # Architecture
//
// The integration tests are split by concern into three sub-packages:
//
//	meta_storage/  — metadata↔storage gRPC interaction (shared cluster via TestMain)
//	failover/   — Raft leader election and crash recovery (per-test clusters)
//	e2e/        — dfsclient.Client SDK end-to-end (per-test clusters)
//
// # Running
//
//	make integration-test
//	# or:
//	go test -tags integration -count=1 -timeout 120s -v ./integration/...
//	go test -tags integration -run TestHeartbeat ./integration/meta_storage/
//
// # Cluster Constructors
//
//	TestCluster (1 meta + N storage):
//	    tc := StartTestCluster(t, 2)
//	    defer tc.Shutdown()
//
//	MultiMetaCluster (N metadata Raft nodes):
//	    mc := StartMultiMetadataCluster(t, 3)
//	    defer mc.Shutdown()
//
//	FullCluster (N meta + N storage, for client SDK tests):
//	    fc := StartFullCluster(t, 3, 3)
//	    defer fc.Shutdown()
//
// # Conventions
//
//	Use unique file/chunk IDs per test to avoid collisions within shared clusters.
//	Call defer cluster.Shutdown() after boot.
//	Use context.WithTimeout for test deadlines.
//	Packages without TestMain should use t.Parallel() where safe.
package testutil

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/internal/retry"
	"github.com/satyam709/distributed-fs/metadata"
	"github.com/satyam709/distributed-fs/storage"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TB is the subset of testing.TB used by cluster constructors.
// *testing.T implements this natively. Packages with TestMain provide
// their own implementation (e.g. a testingTShim) that satisfies TB.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
	TempDir() string
	Cleanup(func())
}

// ---------------------------------------------------------------------------
// Port allocator
// ---------------------------------------------------------------------------

var raftPort atomic.Int32

func init() {
	raftPort.Store(20000 + int32(os.Getpid()%100)*300)
}

func nextRaftAddr() string {
	port := raftPort.Add(1)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// NextRaftAddr returns the next available RAFT address for test clusters.
// Exported for use by per-test cluster constructors.
func NextRaftAddr() string {
	return nextRaftAddr()
}

// ---------------------------------------------------------------------------
// TestCluster — 1 metadata + N storage
// ---------------------------------------------------------------------------

type TestCluster struct {
	MetaApp  *metadata.MetadataApp
	MetaAddr string
	MetaConn *grpc.ClientConn
	MetaC    pb_meta.MetadataServiceClient

	StorageNodes []*storage.StorageNode
	StorageAddrs []string
	StorageConns []*grpc.ClientConn
	StorageCs    []pb_storage.StorageServiceClient
	ReplCs       []pb_storage.ReplicationServiceClient

	cleanups []func()
}

func StartTestCluster(t TB, numStorage int) *TestCluster {
	t.Helper()
	c := &TestCluster{}

	raftDir := t.TempDir()
	metaCfg := metadata.NodeConfig{
		NodeID:            "meta-test-1",
		GRPCAddr:          "127.0.0.1:0",
		RaftAddr:          nextRaftAddr(),
		RaftDir:           raftDir,
		Bootstrap:         true,
		ReplicationFactor: numStorage,
		SuspectTimeout:    3 * time.Second,
		DeadTimeout:       6 * time.Second,
		WatcherInterval:   1 * time.Second,
		ReconcileDelay:    2 * time.Second,
		HeartbeatTimeout:  500 * time.Millisecond,
		ElectionTimeout:   500 * time.Millisecond,
		SnapshotInterval:  30 * time.Second,
		SnapshotThreshold: 8192,
		SnapshotRetain:    1,
	}

	app, err := metadata.NewMetadataApp(metaCfg)
	if err != nil {
		t.Fatalf("NewMetadataApp: %v", err)
	}
	c.MetaApp = app

	ctx := context.Background()
	if err := app.Run(ctx); err != nil {
		t.Fatalf("MetadataApp.Run: %v", err)
	}
	c.MetaAddr = app.BoundGRPCAddr()
	t.Logf("metadata gRPC listening on %s", c.MetaAddr)

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	c.MetaConn, err = grpc.NewClient(c.MetaAddr, opts...)
	if err != nil {
		t.Fatalf("dial metadata: %v", err)
	}
	c.MetaC = pb_meta.NewMetadataServiceClient(c.MetaConn)

	logger := logging.NewCLogger()
	for i := range numStorage {
		node, addr := startStorageNode(t, i, []string{c.MetaAddr}, logger, numStorage, &c.cleanups)

		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			t.Fatalf("dial storage %d: %v", i, err)
		}
		c.StorageNodes = append(c.StorageNodes, node)
		c.StorageAddrs = append(c.StorageAddrs, addr)
		c.StorageConns = append(c.StorageConns, conn)
		c.StorageCs = append(c.StorageCs, pb_storage.NewStorageServiceClient(conn))
		c.ReplCs = append(c.ReplCs, pb_storage.NewReplicationServiceClient(conn))
	}

	time.Sleep(500 * time.Millisecond)
	return c
}

func (c *TestCluster) Shutdown() {
	for _, conn := range c.StorageConns {
		_ = conn.Close()
	}
	if c.MetaConn != nil {
		_ = c.MetaConn.Close()
	}
	for _, n := range c.StorageNodes {
		n.Stop()
	}
	if c.MetaApp != nil {
		_ = c.MetaApp.Shutdown(context.Background())
	}
	for i := len(c.cleanups) - 1; i >= 0; i-- {
		c.cleanups[i]()
	}
}

// ---------------------------------------------------------------------------
// MultiMetaCluster — N metadata Raft nodes
// ---------------------------------------------------------------------------

type MultiMetaCluster struct {
	Apps     []*metadata.MetadataApp
	Addrs    []string
	Conns    []*grpc.ClientConn
	Clients  []pb_meta.MetadataServiceClient
	cleanups []func()
}

func (m *MultiMetaCluster) Shutdown() {
	for _, conn := range m.Conns {
		_ = conn.Close()
	}
	for _, app := range m.Apps {
		_ = app.Shutdown(context.Background())
	}
	for i := len(m.cleanups) - 1; i >= 0; i-- {
		m.cleanups[i]()
	}
}

func StartMultiMetadataCluster(t TB, numNodes int) *MultiMetaCluster {
	t.Helper()
	mc := &MultiMetaCluster{}

	if numNodes < 2 {
		t.Fatalf("StartMultiMetadataCluster requires at least 2 nodes, got %d", numNodes)
	}

	raftAddrs := make(map[string]string, numNodes)
	for i := range numNodes {
		nodeID := fmt.Sprintf("meta-test-%d", i+1)
		raftAddrs[nodeID] = nextRaftAddr()
	}

	for i := range numNodes {
		nodeID := fmt.Sprintf("meta-test-%d", i+1)
		raftDir := t.TempDir()

		peers := make(map[string]string)
		for nid, addr := range raftAddrs {
			if nid != nodeID {
				peers[nid] = addr
			}
		}

		metaCfg := metadata.NodeConfig{
			NodeID:            nodeID,
			GRPCAddr:          "127.0.0.1:0",
			RaftAddr:          raftAddrs[nodeID],
			RaftDir:           raftDir,
			PeerAddrs:         peers,
			Bootstrap:         i == 0,
			ReplicationFactor: 3,
			SuspectTimeout:    10 * time.Second,
			DeadTimeout:       30 * time.Second,
			WatcherInterval:   5 * time.Second,
			ReconcileDelay:    10 * time.Second,
			HeartbeatTimeout:  1 * time.Second,
			ElectionTimeout:   3 * time.Second,
			SnapshotInterval:  120 * time.Second,
			SnapshotThreshold: 8192,
			SnapshotRetain:    1,
		}

		app, err := metadata.NewMetadataApp(metaCfg)
		if err != nil {
			t.Fatalf("NewMetadataApp %d: %v", i, err)
		}
		mc.Apps = append(mc.Apps, app)
	}

	errCh := make(chan error, numNodes)
	type result struct {
		idx int
		app *metadata.MetadataApp
	}
	resCh := make(chan result, numNodes)

	for i, app := range mc.Apps {
		go func(idx int, a *metadata.MetadataApp) {
			if err := a.Run(context.Background()); err != nil {
				errCh <- fmt.Errorf("MetadataApp.Run %d: %w", idx, err)
				return
			}
			resCh <- result{idx: idx, app: a}
		}(i, app)
	}

	runningApps := make([]*metadata.MetadataApp, numNodes)
	for i := 0; i < numNodes; i++ {
		select {
		case err := <-errCh:
			t.Fatalf("%v", err)
		case r := <-resCh:
			runningApps[r.idx] = r.app
		}
	}
	mc.Apps = runningApps

	mc.Addrs = make([]string, numNodes)
	for i, app := range mc.Apps {
		mc.Addrs[i] = app.BoundGRPCAddr()
		nodeID := fmt.Sprintf("meta-test-%d", i+1)
		t.Logf("metadata node %q gRPC on %s raft on %s", nodeID, mc.Addrs[i], raftAddrs[nodeID])
	}

	time.Sleep(2 * time.Second)

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	for _, addr := range mc.Addrs {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			t.Fatalf("dial metadata %s: %v", addr, err)
		}
		mc.Conns = append(mc.Conns, conn)
		mc.Clients = append(mc.Clients, pb_meta.NewMetadataServiceClient(conn))
	}

	return mc
}

func (m *MultiMetaCluster) LeaderIndex() int {
	for i := range m.Apps {
		leaderAddr := m.Apps[i].LeaderRaftAddr()
		if leaderAddr != "" && leaderAddr == m.Apps[i].Config.RaftAddr {
			return i
		}
	}
	return -1
}

func (m *MultiMetaCluster) FollowerIndices() []int {
	leader := m.LeaderIndex()
	var followers []int
	for i := range m.Apps {
		if i != leader {
			followers = append(followers, i)
		}
	}
	return followers
}

// ---------------------------------------------------------------------------
// FullCluster — N metadata + N storage (for client SDK E2E tests)
// ---------------------------------------------------------------------------

type FullCluster struct {
	MetaCluster *MultiMetaCluster

	StorageNodes []*storage.StorageNode
	StorageAddrs []string
	cleanups     []func()
}

func StartFullCluster(t TB, nMeta, nStorage int) *FullCluster {
	t.Helper()
	fc := &FullCluster{}

	fc.MetaCluster = StartMultiMetadataCluster(t, nMeta)
	metaAddrs := slices.Clone(fc.MetaCluster.Addrs)

	logger := logging.NewCLogger()
	for i := range nStorage {
		node, addr := startStorageNode(t, i, metaAddrs, logger, nStorage, &fc.cleanups)
		fc.StorageNodes = append(fc.StorageNodes, node)
		fc.StorageAddrs = append(fc.StorageAddrs, addr)
	}

	time.Sleep(500 * time.Millisecond)
	return fc
}

func (fc *FullCluster) Shutdown() {
	for _, n := range fc.StorageNodes {
		n.Stop()
	}
	fc.MetaCluster.Shutdown()
	for i := len(fc.cleanups) - 1; i >= 0; i-- {
		fc.cleanups[i]()
	}
}

func (fc *FullCluster) MetaAddrs() []string {
	return fc.MetaCluster.Addrs
}

// ---------------------------------------------------------------------------
// startStorageNode — creates and starts a single storage node
// ---------------------------------------------------------------------------

// startStorageNode creates and starts a storage node connected to the given
// metadata addresses. cleanups receives deferred cleanup functions (e.g.,
// closing the checksum store). Returns the started node and its bound address.
func startStorageNode(t TB, i int, metaAddrs []string, logger *logging.CLogger, replicationFactor int, cleanups *[]func()) (*storage.StorageNode, string) {
	t.Helper()
	dir := t.TempDir()

	cs, err := store.NewChecksumIndexBoltDB[[]byte](store.ByteCodec{},
		store.WithDbPath[[]byte](dir),
	)
	if err != nil {
		t.Fatalf("checksum store %d: %v", i, err)
	}
	if err := cs.Open(); err != nil {
		t.Fatalf("checksum store open %d: %v", i, err)
	}
	*cleanups = append(*cleanups, cs.CleanUp)

	ds, err := store.NewDiskStore(
		store.WithChecksumStore(cs),
		store.WithRootDir(dir),
		store.WithTempDir(dir),
		store.WithSplitLevel(2),
		store.WithTotalSpace(256*1024*1024),
	)
	if err != nil {
		t.Fatalf("disk store %d: %v", i, err)
	}

	rp := retry.Policy{MaxAttempts: 3, Base: 100 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2.0}
	mc, err := metaclient.NewMetadataClient(metaAddrs, rp, 10*time.Second)
	if err != nil {
		t.Fatalf("metaclient %d: %v", i, err)
	}

	nodeID := fmt.Sprintf("storage-%d", i)
	cfg := storage.StorageNodeConfig{
		NodeID:            nodeID,
		GRPCAddr:          "127.0.0.1:0",
		MetadataAddrs:     metaAddrs,
		DataDir:           dir,
		Timeout:           30 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		ReplicationFactor: replicationFactor,
		RPCTimeout:        10 * time.Second,
		RetryMaxAttempts:  3,
		RetryBaseBackoff:  100 * time.Millisecond,
		RetryMaxBackoff:   5 * time.Second,
	}

	node, err := storage.NewStorageNode(cfg, logger, ds, mc)
	if err != nil {
		t.Fatalf("NewStorageNode %d: %v", i, err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("StorageNode.Start %d: %v", i, err)
	}

	addr := node.BoundAddr()
	t.Logf("storage node %q listening on %s", nodeID, addr)
	return node, addr
}

// ---------------------------------------------------------------------------
// Utility helpers for writing tests
// ---------------------------------------------------------------------------

func DialStorage(t *testing.T, addr string) pb_storage.StorageServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial storage %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb_storage.NewStorageServiceClient(conn)
}

func PutChunkData(t *testing.T, ctx context.Context, client pb_storage.StorageServiceClient,
	chunkID, fileID string, data []byte, replicaAddrs []string,
) []byte {
	t.Helper()
	stream, err := client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk open stream: %v", err)
	}

	checksum := sha256.Sum256(data)

	var replicas []*pb_storage.NodeInfo
	for _, addr := range replicaAddrs {
		replicas = append(replicas, &pb_storage.NodeInfo{Address: addr})
	}

	err = stream.Send(&pb_storage.PutChunkRequest{
		ChunkId:     chunkID,
		FileId:      fileID,
		Data:        data,
		Checksum:    checksum[:],
		IsLast:      true,
		ReplicateTo: replicas,
	})
	if err != nil {
		t.Fatalf("PutChunk send: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("PutChunk close: %v", err)
	}
	if resp.Response.Code != 200 {
		t.Fatalf("PutChunk unexpected code: %d %s", resp.Response.Code, resp.Response.Msg)
	}
	return checksum[:]
}

// BuildMetaConfig creates a metadata NodeConfig with the given parameters.
func BuildMetaConfig(nodeID, raftAddr, raftDir string, bootstrap bool, replicationFactor int,
	suspectTimeout, deadTimeout, watcherInterval, reconcileDelay time.Duration,
) metadata.NodeConfig {
	return metadata.NodeConfig{
		NodeID:            nodeID,
		GRPCAddr:          "127.0.0.1:0",
		RaftAddr:          raftAddr,
		RaftDir:           raftDir,
		Bootstrap:         bootstrap,
		ReplicationFactor: replicationFactor,
		SuspectTimeout:    suspectTimeout,
		DeadTimeout:       deadTimeout,
		WatcherInterval:   watcherInterval,
		ReconcileDelay:    reconcileDelay,
		HeartbeatTimeout:  500 * time.Millisecond,
		ElectionTimeout:   500 * time.Millisecond,
		SnapshotInterval:  120 * time.Second,
		SnapshotThreshold: 8192,
		SnapshotRetain:    1,
	}
}

// NewMetadataApp creates and returns a MetadataApp from the given config.
func NewMetadataApp(cfg metadata.NodeConfig) (*metadata.MetadataApp, error) {
	return metadata.NewMetadataApp(cfg)
}

// DialMeta dials a metadata node and returns the connection and client.
func DialMeta(addr string) (*grpc.ClientConn, pb_meta.MetadataServiceClient, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("dial metadata: %w", err)
	}
	return conn, pb_meta.NewMetadataServiceClient(conn), nil
}

// StartStorageNodeAt creates and starts a single storage node with a custom
// replication factor. cleanups receives deferred cleanup functions.
func StartStorageNodeAt(t TB, i int, metaAddrs []string, replicationFactor int, cleanups *[]func()) (*storage.StorageNode, string) {
	logger := logging.NewCLogger()
	return startStorageNode(t, i, metaAddrs, logger, replicationFactor, cleanups)
}

// DialStorageFull dials a storage node and returns connection, storage client, and replication client.
func DialStorageFull(addr string) (*grpc.ClientConn, pb_storage.StorageServiceClient, pb_storage.ReplicationServiceClient) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		panic(fmt.Sprintf("dial storage %s: %v", addr, err))
	}
	return conn, pb_storage.NewStorageServiceClient(conn), pb_storage.NewReplicationServiceClient(conn)
}

func GetChunkData(t *testing.T, ctx context.Context, client pb_storage.StorageServiceClient, chunkID string) []byte {
	t.Helper()
	stream, err := client.GetChunk(ctx, &pb_storage.GetChunkRequest{ChunkId: chunkID})
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}

	var buf []byte
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("GetChunk recv: %v", err)
		}
		buf = append(buf, frame.Data...)
		if frame.IsLast {
			break
		}
	}
	return buf
}
