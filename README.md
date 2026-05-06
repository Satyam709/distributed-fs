# Distributed File System in Go

A fault-tolerant distributed file system built in Go, inspired by GFS and HDFS. Separates the control plane (Raft-based metadata cluster) from the data plane (storage nodes) with peer-to-peer chunk replication, automatic failure detection, and self-healing.

---

## Architecture

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

## Quick Start

### Prerequisites

- Go 1.21+
- protoc + protoc-gen-go + protoc-gen-go-grpc plugins
- [buf CLI](https://buf.build/docs/installation/)
- Docker + Docker Compose

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Clone and build

```bash
git clone https://github.com/your-org/distributed-fs.git
cd distributed-fs
go mod download
make proto-gen       # generate protobuf/gRPC code with buf
make build-all       # produces bin/storage, bin/metadata, bin/client/dfs-cli
```

### Run a local cluster

```bash
./scripts/run-cluster.sh -m 1 -s 3 up -d --build
```

Dynamically generates a `docker-compose.yml`, builds images, and starts 1 metadata node + 3 storage nodes in daemon mode.

```bash
# Override node counts
./scripts/run-cluster.sh -m 3 -s 5 up -d --build

# Include a client container
./scripts/run-cluster.sh -m 1 -s 3 -c up -d --build

# View logs
docker compose -f scripts/docker-compose.yml logs -f

# Stop cluster
docker compose -f scripts/docker-compose.yml down
```

### CLI usage

```bash
# Upload a file
./bin/client/dfs-cli upload --file ./data.csv --name data.csv

# List all files
./bin/client/dfs-cli list

# List with prefix filter
./bin/client/dfs-cli list --prefix reports/

# Download a file
./bin/client/dfs-cli download --name data.csv --out ./output/data.csv

# Delete a file
./bin/client/dfs-cli delete --id <file_id>
```

---

## Features

- Raft consensus for replicated, fault-tolerant metadata
- Fixed-size chunking (4 MB) with parallel upload and download
- Peer-to-peer chunk replication — metadata never transfers bytes
- Automatic failure detection via heartbeat timeouts
- Self-healing — under-replicated chunks automatically re-replicated
- SHA256 checksum verification for chunk integrity
- Atomic chunk writes via tmp-file + rename (no partial chunks on disk)
- Resumable uploads via local manifest
- gRPC-based communication across all components

---

## How It Works

### Upload flow

1. Client splits the file into 4 MB chunks, computes chunk IDs as `SHA256(file_id + chunk_index)`, writes a local manifest
2. Client calls `CreateFile` on metadata — receives chunk placement (primary + replicas)
3. Client streams each chunk to its primary storage node in parallel
4. Primary writes locally, fans out replication to replica nodes via P2P
5. Primary calls `CommitChunk` to metadata after replication quorum
6. Client calls `CommitFile` after all chunks are committed

### Download flow

1. Client calls `GetFile` on metadata — receives ordered chunk list with live node addresses
2. Client downloads all chunks in parallel from any live replica per chunk
3. Client verifies SHA256 checksum per chunk, reassembles file in order

### Failure detection and repair

1. Storage nodes send heartbeats every 3 seconds to metadata
2. Metadata marks a node Suspect after 10s of silence, Dead after 30s
3. Dead nodes trigger repair: metadata enqueues jobs, piggybacks them on heartbeat responses to source nodes
4. Source nodes replicate missing chunks to new targets using the same P2P engine as upload
5. Restarted nodes with stale replicas are reconciled — stale chunks evicted automatically

---

## Configuration

Each component is configured via environment variables. See dedicated docs:

| Component | Docs |
|---|---|
| Storage node | [docs/storage.md](docs/storage.md) |
| Metadata node | [docs/metadata.md](docs/metadata.md) |
| Client | [docs/client.md](docs/client.md) |
| Full reference | [docs/configuration.md](docs/configuration.md) |

---

## Repository Structure

```
proto/                        - .proto source files (metadata/v1/, storage/v1/)
gen/proto/                    - generated protobuf/gRPC code
internal/                     - shared packages
  checksum/                   - SHA256 helpers
  errors/                     - typed error definitions
  leaderclient/               - metadata leader resolution and caching
  logging/                    - structured logging
  raftutil/                   - Raft bootstrap utilities
  retry/                      - exponential backoff with jitter
  utils/                      - filepath validation, deep copy
storage/                      - storage node (chunk store, replication, heartbeat, gRPC server)
metadata/                     - metadata node (Raft FSM, BoltDB store, node watcher, repair scheduler, reconciliation)
client/                       - CLI client (chunker, manifest, uploader, downloader)
scripts/                      - run-cluster.sh
integration/                  - integration tests (e2e, failover, meta_storage)
```

---

## gRPC Services

### MetadataService (`proto/metadata/v1/`)
Handles file lifecycle (create, commit, get, delete), chunk placement, node registration, heartbeats, and repair coordination. Called by both clients and storage nodes.

### StorageService (`proto/storage/v1/`)
Client-facing service on storage nodes. Handles chunk upload (client streaming), chunk download (server streaming), eviction, and verification.

### ReplicationService (`proto/storage/v1/`)
Internal P2P service between storage nodes. Handles chunk replication using bidirectional streaming for flow control. Clients never call this directly.

---

## Development

```bash
make test-all          # run all unit tests
make integration-test  # run integration tests (real in-process cluster)
make lint              # lint Go + proto files
make fmt               # format Go + proto files
make proto-gen         # regenerate protobuf/gRPC code with buf
make clean             # remove data and manifest directories
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Go 1.21 |
| Consensus | hashicorp/raft |
| Raft log store | hashicorp/raft-boltdb |
| Metadata persistence | BoltDB (bbolt) |
| RPC framework | gRPC + Protocol Buffers |
| Checksums | crypto/sha256 |
| CLI | cobra |
| Local chunk storage | Disk (two-level sharded directories) |

---

## Design Decisions

- **Raft for metadata** — avoids single point of failure; automatic leader election ensures availability through minority node failures.
- **P2P replication** — the primary storage node owns fan-out, keeping metadata lightweight regardless of data volume. Metadata only records outcomes, never transfers bytes.
- **Fixed-size chunking** — predictable and simple. Content-defined chunking adds complexity without benefit for a write-once system without deduplication.
- **Atomic rename for chunk writes** — writing to `.tmp` then renaming guarantees a crash never leaves a corrupt visible chunk.
- **Piggyback repair on heartbeats** — storage nodes already call metadata every 3s; using that channel for repair delivery avoids metadata maintaining outbound connections.

---

## Limitations

This is an academic implementation. Known simplifications:

- Write-once model — no file updates or appends
- No rack-awareness in placement strategy
- No TLS — all gRPC connections are insecure
- No authentication or access control
- Single metadata cluster — no cross-datacenter replication
- No quota management

---

## References

- [The Google File System (Ghemawat et al., 2003)](https://research.google/pubs/pub51/)
- [HDFS Architecture Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [In Search of an Understandable Consensus Algorithm — Raft (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf)
- [hashicorp/raft](https://github.com/hashicorp/raft)
