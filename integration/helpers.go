//go:build integration

// Package integration contains integration tests that exercise the full
// metadata ↔ storage node interaction over real gRPC. Tests in this package
// start a single-node Raft metadata cluster and multiple storage nodes
// in-process, mimicking a production topology without Docker.
//
// Run with: go test -tags integration -count=1 -timeout 120s ./integration/
package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata"
	"github.com/satyam709/distributed-fs/storage"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------------------------------------------------------------------------
// Port allocator — deterministic, no collisions within a test run.
// ---------------------------------------------------------------------------

var raftPort atomic.Int32

func init() {
	raftPort.Store(19100)
}

func nextRaftAddr() string {
	port := raftPort.Add(1)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

// ---------------------------------------------------------------------------
// TB — minimal interface satisfied by both *testing.T and testingTShim.
// ---------------------------------------------------------------------------

// TB is the subset of testing.TB used by StartTestCluster. This allows
// the function to be called from TestMain (which has no *testing.T) via
// a lightweight shim, as well as from normal test functions.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
	TempDir() string
	Cleanup(func())
}

// ---------------------------------------------------------------------------
// TestCluster — a mini DFS cluster running inside a test process.
// ---------------------------------------------------------------------------

// TestCluster holds a running metadata node, multiple storage nodes,
// and pre-dialled gRPC client connections to each.
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

	// cleanups tracks functions to run on shutdown (in LIFO order).
	cleanups []func()
}

// StartTestCluster boots a 1-metadata + numStorage storage-node cluster.
// The metadata node uses single-node Raft (bootstrap=true). Storage nodes
// auto-register on Start() and begin heartbeating.
//
// The caller must call cluster.Shutdown() when done (typically via t.Cleanup).
func StartTestCluster(t TB, numStorage int) *TestCluster {
	t.Helper()
	c := &TestCluster{}

	// ── Metadata Node ──────────────────────────────────────────────
	raftDir := t.TempDir()
	metaCfg := metadata.NodeConfig{
		NodeID:            "meta-test-1",
		GRPCAddr:          "127.0.0.1:0",
		RaftAddr:          nextRaftAddr(),
		RaftDir:           raftDir,
		Bootstrap:         true,
		ReplicationFactor: numStorage, // match storage count
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

	// Dial metadata.
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	c.MetaConn, err = grpc.NewClient(c.MetaAddr, opts...)
	if err != nil {
		t.Fatalf("dial metadata: %v", err)
	}
	c.MetaC = pb_meta.NewMetadataServiceClient(c.MetaConn)

	// ── Storage Nodes ──────────────────────────────────────────────
	logger := logging.NewCLogger()
	for i := range numStorage {
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
		c.cleanups = append(c.cleanups, cs.CleanUp)

		ds, err := store.NewDiskStore(
			store.WithChecksumStore(cs),
			store.WithRootDir(dir),
			store.WithTempDir(dir),
			store.WithSplitLevel(2),
			store.WithTotalSpace(256*1024*1024), // 256 MB virtual
		)
		if err != nil {
			t.Fatalf("disk store %d: %v", i, err)
		}

		mc, err := metaclient.NewMetadataClient(c.MetaAddr)
		if err != nil {
			t.Fatalf("metaclient %d: %v", i, err)
		}

		nodeID := fmt.Sprintf("storage-%d", i)
		cfg := storage.StorageNodeConfig{
			NodeID:            nodeID,
			GRPCAddr:          "127.0.0.1:0",
			MetadataAddr:      c.MetaAddr,
			DataDir:           dir,
			Timeout:           30 * time.Second,
			HeartbeatInterval: 1 * time.Second,
			ReplicationFactor: numStorage,
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

		c.StorageNodes = append(c.StorageNodes, node)
		c.StorageAddrs = append(c.StorageAddrs, addr)

		// Dial storage.
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			t.Fatalf("dial storage %d: %v", i, err)
		}
		c.StorageConns = append(c.StorageConns, conn)
		c.StorageCs = append(c.StorageCs, pb_storage.NewStorageServiceClient(conn))
		c.ReplCs = append(c.ReplCs, pb_storage.NewReplicationServiceClient(conn))
	}

	// Give storage nodes a moment to complete registration & first heartbeat.
	time.Sleep(500 * time.Millisecond)

	return c
}

// Shutdown tears down the cluster in reverse order: close client connections,
// stop storage nodes, then stop metadata.
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
	// Run registered cleanups in LIFO order.
	for i := len(c.cleanups) - 1; i >= 0; i-- {
		c.cleanups[i]()
	}
}

// ---------------------------------------------------------------------------
// Utility helpers for writing tests.
// ---------------------------------------------------------------------------

// DialStorage creates a new storage gRPC client to the given address.
func DialStorage(t *testing.T, addr string) pb_storage.StorageServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial storage %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb_storage.NewStorageServiceClient(conn)
}

// PutChunkData uploads a byte slice as a single-frame chunk to the given
// storage service client and returns the checksum. The chunkID is set by
// the caller. replicaAddrs optionally lists addresses for fan-out.
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

// GetChunkData downloads a chunk from the given storage service client
// and returns the reassembled bytes.
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
