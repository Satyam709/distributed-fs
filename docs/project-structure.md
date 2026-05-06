## Folder Structure

```
distributed-fs/
│
├── go.mod
├── go.sum
├── Makefile                        ← build, run, test, proto-gen commands
├── README.md
├── buf.yaml                        ← buf protobuf configuration
├── buf.gen.yaml                    ← buf code generation configuration
│
├── proto/                          ← all .proto files, single source of truth
│   ├── buf.yaml
│   ├── metadata/
│   │   └── v1/
│   │       └── metadata.proto
│   └── storage/
│       └── v1/
│           └── storage.proto        ← StorageService + ReplicationService
│
├── gen/                            ← generated proto code, never edit manually
│   └── proto/
│       ├── metadata/
│       │   └── v1/
│       │       ├── metadata.pb.go
│       │       └── metadata_grpc.pb.go
│       └── storage/
│           └── v1/
│               ├── storage.pb.go
│               └── storage_grpc.pb.go
│
├── internal/                       ← shared code, not exported outside monorepo
│   ├── checksum/
│   │   └── checksum.go             ← SHA256 helpers
│   ├── errors/
│   │   └── errors.go               ← typed error definitions
│   ├── leaderclient/
│   │   ├── client.go               ← metadata leader resolver
│   │   └── leader_cache.go         ← leader address cache
│   ├── logging/
│   │   └── logger.go               ← structured logging (CLogger)
│   ├── raftutil/
│   │   └── raftutil.go             ← Raft bootstrap utilities
│   ├── retry/
│   │   └── retry.go                ← RetryPolicy — exponential backoff
│   └── utils/
│       ├── deep_copy.go            ← deep copy utilities
│       └── validate-filepath.go    ← filepath validation
│
├── storage/                        ← storage node (data plane)
│   ├── Dockerfile
│   ├── cmd/
│   │   └── main.go                 ← storage node entrypoint
│   ├── config.go                   ← StorageNodeConfig
│   ├── node.go                     ← StorageNode root struct, Start/Stop
│   ├── chunk/
│   │   └── writer.go               ← ChunkWriter
│   ├── metaclient/
│   │   └── client.go               ← gRPC client to metadata
│   ├── replication/
│   │   ├── manager.go              ← ReplicationManager
│   │   ├── peer_dialer.go          ← PeerDialer (connection pool)
│   │   └── retry.go                ← replication RetryPolicy
│   ├── server/
│   │   └── server.go               ← gRPC server setup + handlers
│   ├── service/
│   │   ├── heartbeat.go            ← HeartbeatSender
│   │   ├── register.go             ← Registration logic
│   │   └── types.go                ← Service types
│   └── store/
│       ├── interface.go            ← ChunkStore interface
│       ├── disk.go                 ← DiskChunkStore
│       ├── memory.go               ← in-memory impl for tests
│       └── checksum_index.go       ← BoltDB ChecksumIndex
│
├── metadata/                       ← metadata node (control plane)
│   ├── Dockerfile
│   ├── cmd/
│   │   └── main.go
│   ├── app.go                      ← MetadataApp root struct, Start/Stop
│   ├── config.go                   ← NodeConfig
│   ├── raft.go                     ← RaftNode wrapper
│   ├── fsm/
│   │   ├── fsm.go                  ← MetadataFSM — Apply, Snapshot, Restore
│   │   ├── commands.go             ← all MetadataCommand types
│   │   └── types.go                ← FileRecord, ChunkRecord, NodeEntry, RepairJob
│   ├── placement/
│   │   └── strategy.go             ← PlacementStrategy
│   ├── reconciler/
│   │   └── reconciler.go           ← Node reconciliation
│   ├── scheduler/
│   │   └── repair_scheduler.go     ← RepairScheduler
│   ├── server/
│   │   ├── server.go               ← gRPC server setup
│   │   ├── handler.go              ← main handler with leader check
│   │   ├── file_handler.go         ← file operations
│   │   ├── chunk_handlers.go       ← chunk operations
│   │   ├── node_handlers.go        ← node registration/heartbeat
│   │   └── repair_handlers.go      ← repair reporting
│   ├── store/
│   │   └── boltstore.go            ← BoltDB Raft log store
│   └── watcher/
│       └── node_watcher.go         ← NodeWatcher
│
├── client/                         ← CLI client
│   ├── cmd/
│   │   └── dfs-cli/
│   │       └── main.go             ← Cobra CLI entrypoint
│   ├── dfsclient/
│   │   ├── client.go               ← DFSClient main struct
│   │   ├── options.go              ← client options
│   │   └── types.go                ← ProgressInfo, UploadResult, etc.
│   └── internal/
│       ├── chunker/
│       │   └── chunker.go          ← file splitter, chunk_id generation
│       ├── dfsclientconfig/
│       │   └── config.go           ← configuration
│       ├── manifest/
│       │   └── manifest.go         ← local manifest, resume support
│       ├── metadataclient/
│       │   ├── client.go           ← MetadataClient interface
│       │   ├── grpc.go             ← gRPC implementation
│       │   └── mock.go             ← mock for tests
│       ├── storageclient/
│       │   ├── client.go           ← StorageClient interface
│       │   ├── grpc.go             ← gRPC implementation
│       │   └── mock.go             ← mock for tests
│       └── service/
│           ├── upload.go           ← ParallelUploader
│           ├── download.go         ← ParallelDownloader
│           └── list.go             ← ListFiles logic
│
├── integration/                    ← integration tests
│   ├── testutil/
│   │   └── helpers.go              ← TestCluster setup helpers
│   ├── e2e/
│   │   └── e2e_test.go             ← end-to-end tests
│   ├── failover/
│   │   └── failover_test.go        ← metadata failover tests
│   └── meta_storage/
│       ├── main_test.go            ← TestMain: starts cluster
│       ├── registration_test.go    ← node registration tests
│       ├── upload_download_test.go ← upload/download round-trip
│       ├── file_ops_test.go        ← file operations tests
│       ├── replication_test.go     ← replication tests
│       ├── chunksize_test.go       ← chunk size edge cases
│       ├── checksum_test.go        ← checksum verification
│       └── repair_after_node_death_test.go ← repair tests
│
├── scripts/
│   └── run-cluster.sh              ← spin up full cluster via docker compose
│
├── bin/                            ← build output (gitignored except .gitkeep)
└── data/                           ← runtime data (gitignored, used locally)
```
