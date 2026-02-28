# Distributed File System in Go

A fault-tolerant distributed file system built in Go, inspired by GFS and HDFS. The system separates the control plane (metadata cluster) from the data plane (storage nodes), implements peer-to-peer chunk replication, and provides automatic failure detection and repair.

---

## Architecture Overview

```
                        ┌─────────────────────────────┐
                        │      Metadata Cluster (Raft)│
                        │   Meta1 ←→ Meta2 ←→ Meta3   │
                        └──────────────┬──────────────┘
                                       │
               ┌───────────────────────┼───────────────────────┐
               │ heartbeat             │ placement             │ repair
               ▼                       ▼                       ▼
        ┌──────────┐           ┌──────────┐           ┌──────────┐
        │ Storage  │◄─────────►│ Storage  │◄─────────►│ Storage  │
        │  Node 1  │           │  Node 2  │           │  Node 3  │
        └──────────┘           └──────────┘           └──────────┘
               ▲
               │ upload / download
               │
           Client CLI
```

**Control Plane** — Raft-based metadata cluster. Stores file-to-chunk mappings, chunk-to-node mappings, node health, and replication state. Never touches chunk data.

**Data Plane** — Storage nodes handle chunk storage, P2P replication between peers, checksum validation, and heartbeating to metadata.

**Client** — CLI that splits files into chunks, coordinates with metadata for placement, and streams chunks directly to/from storage nodes in parallel.

---

## Features

- Replicated metadata using Raft consensus (hashicorp/raft)
- Fixed-size chunking with parallel upload and download
- Peer-to-peer chunk replication — metadata never transfers bytes
- Automatic failure detection via heartbeat timeouts
- Self-healing — under-replicated chunks are automatically re-replicated
- Chunk integrity verification via SHA256 checksums
- Atomic chunk writes — no partial chunks ever visible on disk
- Resumable uploads via local manifest
- Node reconciliation on restart — stale replicas evicted automatically
- gRPC-based communication across all components

---

## Repository Structure

```
distributed-fs/
│
├── proto/                          # .proto source files
│   ├── metadata/metadata.proto
│   ├── storage/storage.proto
│   └── replication/replication.proto
│
├── gen/                            # generated gRPC/protobuf code (committed)
│   ├── metadata/
│   ├── storage/
│   └── replication/
│
├── internal/                       # shared packages
│   ├── types/                      # common types: NodeInfo, ChunkID, errors
│   ├── checksum/                   # SHA256 helpers
│   └── retry/                      # exponential backoff with jitter
│
├── storage/                        # storage node
│   ├── cmd/main.go                 # binary entrypoint
│   ├── store/                      # ChunkStore interface + DiskChunkStore
│   ├── chunk/                      # ChunkWriter, ChunkReader
│   ├── replication/                # ReplicationManager, PeerDialer, repair workers
│   ├── server/                     # gRPC handlers (StorageService, ReplicationService)
│   ├── heartbeat/                  # HeartbeatSender
│   ├── node.go                     # StorageNode root struct
│   └── config.go
│
├── metadata/                       # metadata node
│   ├── cmd/main.go
│   ├── fsm/                        # Raft FSM — Apply, Snapshot, Restore
│   ├── store/                      # BoltDB Raft log store
│   ├── watcher/                    # NodeWatcher — heartbeat timeout detection
│   ├── scheduler/                  # RepairScheduler, placement strategy
│   ├── server/                     # MetadataService gRPC handler
│   ├── node.go
│   └── config.go
│
├── client/                         # CLI client
│   ├── cmd/main.go                 # Cobra CLI entrypoint
│   ├── chunker/                    # file splitter, chunk_id generation
│   ├── manifest/                   # local upload manifest, resume support
│   ├── uploader/                   # ParallelUploader
│   ├── downloader/                 # ParallelDownloader
│   └── config.go
│
├── scripts/
│   ├── start-cluster.sh            # spin up full cluster locally
│   └── demo.sh                     # demo: upload → kill node → verify repair
│
├── docker/
│   ├── storage.Dockerfile
│   ├── metadata.Dockerfile
│   └── docker-compose.yml
│
├── Makefile
└── README.md
```

---

## Prerequisites

- Go 1.21+
- protoc (Protocol Buffer compiler)
- protoc-gen-go and protoc-gen-go-grpc plugins
- Docker + Docker Compose (optional, for containerized demo)

Install protoc plugins:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

---

## Getting Started

### 1. Clone and install dependencies

```bash
git clone https://github.com/your-org/distributed-fs.git
cd distributed-fs
go mod download
```

### 2. Generate protobuf code

```bash
make proto-gen
```

### 3. Build all binaries

```bash
make build-all
```

Produces three binaries: `bin/storage`, `bin/metadata`, `bin/client`

### 4. Start a local cluster

```bash
make run-cluster
```

This starts 3 metadata nodes (Raft cluster) and 4 storage nodes on localhost using the configs in `scripts/`.

### 5. Use the CLI

```bash
# upload a file
./bin/client upload --file ./testdata/sample.txt --name sample.txt

# list files
./bin/client list

# download a file
./bin/client download --name sample.txt --out ./output/sample.txt
```

---

## Configuration

### Storage Node (`storage/config.go`)

| Field | Default | Description |
|---|---|---|
| `node_id` | required | Unique identifier for this node |
| `data_dir` | `/data` | Root directory for chunk storage |
| `grpc_addr` | `:7001` | Address to listen on |
| `meta_addrs` | required | Comma-separated metadata node addresses |
| `chunk_size` | `4194304` | Chunk size in bytes (4MB) |
| `max_outbound_streams` | `8` | Max concurrent outbound replication streams |
| `heartbeat_interval` | `3s` | How often to send heartbeat to metadata |
| `replication_timeout` | `30s` | Timeout for full replication fan-out |

### Metadata Node (`metadata/config.go`)

| Field | Default | Description |
|---|---|---|
| `node_id` | required | Unique identifier for this node |
| `grpc_addr` | `:9000` | gRPC address for clients and storage nodes |
| `raft_addr` | `:9001` | Raft internal communication address |
| `raft_dir` | `/raft` | Directory for Raft log and snapshots |
| `replication_factor` | `3` | Target replica count per chunk |
| `suspect_timeout` | `9s` | Time before node marked suspect |
| `dead_timeout` | `15s` | Time before node marked dead |
| `reconcile_delay` | `30s` | Delay before reconciling a rejoining node |

---

## gRPC Services

### MetadataService
Handles file lifecycle, chunk placement, node registration, heartbeats, and repair coordination. Clients and storage nodes both talk to this service.

### StorageService
Client-facing service on storage nodes. Handles chunk upload (client streaming), chunk download (server streaming), eviction, and verification.

### ReplicationService
Internal service between storage nodes only. Handles peer-to-peer chunk transfer using bidirectional streaming for flow control. Clients never call this directly.

Full method specifications are documented in `proto/`.

---

## How It Works

### Upload

1. Client generates a `file_id` (UUID) and splits the file into 4MB chunks
2. Chunk IDs computed as `SHA256(file_id + chunk_index)` — no server needed
3. Client writes a local manifest for resumability
4. Client calls `CreateFile` on metadata — receives placement per chunk (primary + 2 replicas)
5. Client streams each chunk to its primary storage node in parallel
6. Primary node writes to disk, then fans out replication to replica nodes concurrently via P2P
7. Primary calls `CommitChunk` to metadata after replication quorum is met
8. Client calls `CommitFile` after all chunks are committed

### Download

1. Client calls `GetFile` on metadata — receives ordered chunk list with live node addresses
2. Client downloads all chunks in parallel from any live replica per chunk
3. Client verifies SHA256 checksum per chunk
4. Client reassembles file in chunk_index order

### Failure Detection and Repair

1. Storage nodes send heartbeats every 3 seconds
2. Metadata marks a node Suspect after 9s of silence, Dead after 15s
3. On death, metadata scans all chunks on the dead node and enqueues repair jobs
4. Repair jobs are delivered to live source nodes via heartbeat response piggyback
5. Source node replicates chunk to a new target node — same P2P engine as upload
6. If a node restarts after repair ran, stale replicas are evicted during reconciliation

### Replication Factor Maintenance

The system continuously ensures every chunk has exactly `replication_factor` live replicas. Under-replication triggers repair. Over-replication (from node restart after repair) triggers eviction. The canonical replica list in the metadata FSM is always the source of truth.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Go 1.21 |
| Consensus | hashicorp/raft |
| Raft log store | hashicorp/raft-boltdb |
| Metadata persistence | BoltDB (bbolt) |
| RPC framework | gRPC + Protocol Buffers |
| Checksums | stdlib crypto/sha256 |
| CLI | cobra |
| Local chunk storage | Disk (two-level sharded directories) |

---

## Development

### Running tests

```bash
make test-all          # all packages
make test-storage      # storage node only
make test-metadata     # metadata node only
make test-client       # client only
```

### Regenerating proto code

Edit `.proto` files in `proto/`, then:

```bash
make proto-gen
```

Never edit files in `gen/` manually.

### Running the demo

The demo script starts a full cluster, uploads a file, kills a storage node, waits for repair, then verifies the file is still downloadable:

```bash
make demo
```

---

## Design Decisions

**Why Raft for metadata?** A single metadata node is a single point of failure — the entire system becomes unavailable even though all chunk data is intact. Raft ensures metadata survives any minority of node failures with automatic leader election and no data loss.

**Why P2P replication instead of metadata-driven?** If metadata drove every replication step, it would become a bottleneck at high upload throughput. By having the primary storage node own the fan-out, metadata stays lightweight regardless of data volume — it only records the outcome, never participates in byte transfer.

**Why fixed-size chunking?** Simplicity and predictability. Content-defined chunking enables deduplication but adds significant complexity. For a write-once system without dedup requirements, fixed-size is the right tradeoff.

**Why atomic rename for chunk writes?** Rename is atomic on Linux. Writing to a `.tmp` file first and then renaming means a crash at any point during a write never leaves a corrupt visible chunk. Either the full chunk exists or nothing does.

**Why piggyback repair jobs on heartbeat responses?** Avoids metadata having to maintain outbound connections to storage nodes. Storage nodes already call metadata every 3 seconds — using that channel for repair delivery simplifies the connection topology significantly.

---

## Limitations

This is an academic implementation. Known simplifications compared to production systems:

- Write-once model — no file updates or appends
- No rack-awareness in placement strategy
- No TLS — all gRPC connections are insecure
- No authentication or access control
- Single metadata cluster — no cross-datacenter replication
- No quota management per user or directory

---

## References

- [The Google File System (Ghemawat et al., 2003)](https://research.google/pubs/pub51/)
- [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [In Search of an Understandable Consensus Algorithm — Raft paper (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)
- [hashicorp/raft](https://github.com/hashicorp/raft)