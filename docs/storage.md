# Storage Node

## Overview

The Storage Node is the data plane component of the distributed file system. It stores file chunks on disk, participates in peer-to-peer replication for durability, reports health to metadata nodes via heartbeats, and executes repair operations when instructed. Each storage node is identified by a persistent UUID and exposes gRPC APIs for client chunk operations and peer-to-peer replication.

---

## Configuration

### Config Loading Priority

1. **Defaults** — struct defaults applied first
2. **JSON file** — loaded if `STORAGE_CONFIG` env var points to a valid path
3. **Environment variables** — `STORAGE_*` prefixed vars override corresponding fields
4. **Validate** — configuration is validated for correctness

### Environment Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `STORAGE_NODE_ID` | string | auto-generated UUID | Unique node identifier |
| `STORAGE_GRPC_ADDR` | string | `:4000` | gRPC listen address |
| `STORAGE_ADVERTISE_ADDR` | string | (empty) | Address advertised to metadata/peers for Docker/NAT |
| `STORAGE_METADATA_ADDRS` | string | `:3000` | Comma-separated metadata node addresses |
| `STORAGE_DATA_DIR` | string | `./data` | Root directory for chunk storage |
| `STORAGE_REPLICATION_FACTOR` | int | `3` | Target replica count per chunk |
| `STORAGE_TIMEOUT` | duration | `120s` | General operation timeout |
| `STORAGE_HEARTBEAT_INTERVAL` | duration | `3s` | Interval between heartbeat messages |
| `STORAGE_RPC_TIMEOUT` | duration | `10s` | Per-RPC call timeout |
| `STORAGE_RETRY_MAX_ATTEMPTS` | int | `5` | Maximum retry attempts |
| `STORAGE_RETRY_BASE_BACKOFF` | duration | `100ms` | Initial retry backoff |
| `STORAGE_RETRY_MAX_BACKOFF` | duration | `5s` | Maximum retry backoff |
| `STORAGE_CONFIG` | string | — | Path to JSON config file |

### JSON Config Example

```json
{
  "grpc_addr": ":4000",
  "advertise_addr": "storage-1:4000",
  "metadata_addrs": ["metadata-1:4001"],
  "timeout": "120s",
  "heartbeat_interval": "3s",
  "data_dir": "/storage_node/data",
  "replication_factor": 3,
  "rpc_timeout": "10s",
  "retry_max_attempts": 5,
  "retry_base_backoff": "100ms",
  "retry_max_backoff": "5s"
}
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Storage Node                            │
│                                                              │
│  ┌─────────────┐  ┌──────────────────┐  ┌────────────────┐  │
│  │ gRPC Server │  │ HeartbeatSender  │  │ ReplicationMgr │  │
│  │  (server/)  │  │  (service/)      │  │ (replication/) │  │
│  └──────┬──────┘  └────────┬─────────┘  └───────┬────────┘  │
│         │                  │                    │             │
│         │     ┌────────────┴──────────┐         │             │
│         │     │    MetaClient          │         │             │
│         │     │   (metaclient/)        │         │             │
│         │     └────────────┬──────────┘         │             │
│         │                  │                    │             │
│  ┌──────┴──────────────────┴────────────────────┴──────┐      │
│  │                 ChunkStore (store/)                  │      │
│  │  DiskChunkStore + BoltDB ChecksumIndex              │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                              │
│  Disk Layout:                                                │
│  <DataDir>/node_id            (persisted UUID)                │
│  <DataDir>/xx/yy/chunk_id     (two-level sharded chunks)     │
│  <DataDir>/tmp/               (in-flight writes)             │
│  <DataDir>/checksums.bolt     (BoltDB checksum index)        │
└──────────────────────────────────────────────────────────────┘
```

---

## Startup Sequence

1. **Load configuration** — defaults → JSON file → environment variables → validate
2. **Load or generate NodeID** — read from `<DataDir>/node_id`, or generate UUID and persist
3. **Initialize chunk store** — create directory structure, open BoltDB checksum index
4. **Scan chunk inventory** — walk data dir, collect chunk IDs and checksums
5. **Connect to metadata** — dial configured metadata addresses
6. **Register with metadata** — send `RegisterNode` with address and chunk inventory
7. **Start heartbeat sender** — periodic heartbeat every 3s
8. **Start gRPC server** — bind to `GRPCAddr`, serve StorageService and ReplicationService
9. **Start replication manager** — initialize peer connection pool

---

## Chunk Storage

### Directory Layout

Two-level prefix sharding to avoid inode limits at scale:

```
<DataDir>/
├── node_id              # persisted UUID
├── checksums.bolt       # BoltDB checksum index (chunk_id → SHA256)
├── tmp/                 # staging area for atomic writes
└── ab/
    └── 12/
        └── ab12fe93d4....chunk
```

### Write Strategy

1. Write incoming chunk data to `<DataDir>/tmp/<random>.tmp`
2. Compute SHA256 checksum simultaneously during write (single pass)
3. `fsync` the temp file
4. Atomically `rename` the temp file to its final sharded path
5. Store checksum in BoltDB index

The atomic rename guarantees no reader ever sees a partial chunk.

### Checksum Index

BoltDB-backed key-value store at `<DataDir>/checksums.bolt`:
- **Written** after every chunk upload
- **Read** during chunk download for verification
- **Deleted** when a chunk is evicted

---

## Heartbeat and Repair

### Heartbeat Loop

Every 3 seconds, `HeartbeatSender` sends node health (free space, chunk count) to metadata. The heartbeat response may carry repair instructions piggybacked by the metadata node.

### Repair Execution

When a heartbeat response includes repair jobs:
1. For each job, determine if the node needs to **push** a chunk it holds or **pull** a missing chunk
2. Use `ReplicationManager` + `PeerDialer` to establish gRPC connections to target peers
3. Apply retry policy (exponential backoff: 100ms base, 5s max, 5 attempts)
4. Report result in next heartbeat

---

## Replication Flow

When a client uploads a chunk:
1. Client sends chunk to the **primary** storage node (selected by metadata)
2. Primary writes chunk locally, computes checksum
3. Primary fans out `ReplicateChunk` RPCs to replica nodes concurrently
4. After replicas acknowledge (quorum met), primary calls `CommitChunk` on metadata
5. Primary returns success to client

`CommitChunk` is always called by the storage node, never by the client.

---

## Shutdown

1. Stop heartbeat sender
2. Graceful stop gRPC server (drains active RPCs)
3. Close peer connections
4. Close BoltDB checksum index

---

## Package Structure

| Package | Path | Responsibility |
|---|---|---|
| `main` | `storage/cmd/main.go` | Entry point, wires components |
| `storage` | `storage/config.go` | Configuration struct and loading |
| `storage` | `storage/node.go` | `StorageNode` struct, Start/Stop lifecycle |
| `chunk` | `storage/chunk/writer.go` | `ChunkWriter` — streaming chunk writes |
| `metaclient` | `storage/metaclient/client.go` | gRPC client to metadata service |
| `replication` | `storage/replication/manager.go` | Coordinates replication and repair |
| `replication` | `storage/replication/peer_dialer.go` | Peer connection pool with health checks |
| `replication` | `storage/replication/retry.go` | Exponential backoff retry policy |
| `server` | `storage/server/server.go` | gRPC server setup and handler registration |
| `service` | `storage/service/heartbeat.go` | Periodic heartbeat sender |
| `service` | `storage/service/register.go` | Startup registration with metadata |
| `store` | `storage/store/interface.go` | `ChunkStore` interface |
| `store` | `storage/store/disk.go` | `DiskChunkStore` implementation |
| `store` | `storage/store/memory.go` | In-memory store for tests |
| `store` | `storage/store/checksum_index.go` | BoltDB-backed checksum index |
