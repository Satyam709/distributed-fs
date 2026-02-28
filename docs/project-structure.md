## Folder Structure

```
distributed-fs/
│
├── go.mod
├── go.sum
├── Makefile                        ← build, run, test, proto-gen commands
├── README.md
│
├── proto/                          ← all .proto files, single source of truth
│   ├── metadata/
│   │   └── metadata.proto
│   ├── storage/
│   │   └── storage.proto
│   └── replication/
│       └── replication.proto
│
├── gen/                            ← generated proto code, never edit manually
│   ├── metadata/
│   │   ├── metadata.pb.go
│   │   └── metadata_grpc.pb.go
│   ├── storage/
│   │   ├── storage.pb.go
│   │   └── storage_grpc.pb.go
│   └── replication/
│       ├── replication.pb.go
│       └── replication_grpc.pb.go
│
├── internal/                       ← shared code, not exported outside monorepo
│   ├── types/
│   │   └── types.go                ← ChunkID, NodeInfo, ReplicaResult, errors etc.
│   ├── checksum/
│   │   └── checksum.go             ← SHA256 helpers
│   └── retry/
│       └── retry.go                ← RetryPolicy (used by storage + client)
│
├── storage/                        ← Person A + B (storage node)
│   ├── cmd/
│   │   └── main.go                 ← storage node entrypoint
│   ├── store/
│   │   ├── interface.go            ← ChunkStore interface
│   │   ├── disk.go                 ← DiskChunkStore
│   │   ├── memory.go               ← in-memory impl for tests
│   │   └── checksum_index.go       ← ChecksumIndex (BoltDB)
│   ├── chunk/
│   │   ├── writer.go               ← ChunkWriter
│   │   └── reader.go               ← ChunkReader
│   ├── replication/
│   │   ├── manager.go              ← ReplicationManager
│   │   ├── dialer.go               ← PeerDialer
│   │   └── worker.go               ← repair worker pool
│   ├── server/
│   │   ├── storage_handler.go      ← StorageService gRPC handler
│   │   ├── replication_handler.go  ← ReplicationService gRPC handler
│   │   └── server.go               ← gRPC server setup
│   ├── heartbeat/
│   │   └── sender.go               ← HeartbeatSender
│   ├── node.go                     ← StorageNode root struct, wiring
│   └── config.go                   ← NodeConfig
│
├── metadata/                       ← week 2
│   ├── cmd/
│   │   └── main.go
│   ├── fsm/
│   │   ├── fsm.go                  ← MetadataFSM, Apply, Snapshot, Restore
│   │   └── commands.go             ← all MetadataCommand types
│   ├── store/
│   │   └── boltstore.go            ← BoltDB Raft log store
│   ├── watcher/
│   │   └── node_watcher.go         ← NodeWatcher, heartbeat timeout detection
│   ├── scheduler/
│   │   └── repair_scheduler.go     ← RepairScheduler, placement strategy
│   ├── server/
│   │   └── metadata_handler.go     ← MetadataService gRPC handler
│   ├── node.go                     ← MetadataNode root struct
│   └── config.go
│
├── client/                         ← Person B, second half of week
│   ├── cmd/
│   │   └── main.go                 ← CLI entrypoint (Cobra)
│   ├── chunker/
│   │   └── chunker.go              ← file splitter, chunk_id generation
│   ├── manifest/
│   │   └── manifest.go             ← local upload manifest, resume support
│   ├── uploader/
│   │   └── uploader.go             ← ParallelUploader
│   ├── downloader/
│   │   └── downloader.go           ← ParallelDownloader
│   └── config.go
│
├── scripts/
│   ├── start-cluster.sh            ← spin up metadata + storage nodes locally
│   └── demo.sh                     ← demo script: upload, kill node, verify repair
│
└── docker/                         ← optional, for demo
    ├── storage.Dockerfile
    ├── metadata.Dockerfile
    └── docker-compose.yml
```