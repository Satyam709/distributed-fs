# Distributed File System — Project Details

## Overview

This project implements a fault-tolerant distributed file system with:
- Replicated metadata using Raft consensus (hashicorp/raft)
- Peer-to-peer chunk replication between storage nodes
- gRPC-based inter-service communication
- Client-side parallel chunk transfer
- Automatic failure detection and re-replication
- Chunk integrity verification via SHA256 checksums
- Resumable uploads via local manifest

The system separates the control plane (metadata cluster) from the data plane (storage nodes). Architecture is inspired by GFS/HDFS.

---

## Architecture

### Control Plane (Metadata Cluster)

- Raft-based metadata cluster (1-5 nodes)
- File → chunk mapping
- Chunk → node mapping
- Node health tracking
- Replication decisions
- Placement strategy
- Repair scheduling
- Node reconciliation

**Why Raft:** Strong consistency, automatic leader election, no single point of failure.

### Data Plane (Storage Nodes)

- Chunk storage on disk
- Chunk transfer to clients
- P2P replication between peers
- Checksum validation
- Heartbeat to metadata
- Repair execution

---

## gRPC Services

### MetadataService (`proto/metadata/v1/metadata.proto`)

| Method | Caller | Description |
|---|---|---|
| `CreateFile` | Client | Create file with chunk list, return placements |
| `GetFile` | Client | File metadata + chunk list with live node addresses |
| `DeleteFile` | Client | Delete file, enqueue chunk eviction |
| `ListFiles` | Client | List files with optional prefix filter |
| `CommitFile` | Client | Mark file as complete |
| `CommitChunk` | Storage node | Update chunk with confirmed replica nodes |
| `GetChunkLocations` | Client | Live node addresses for a chunk |
| `ReportCorruption` | Storage node | Remove node from replica list, trigger repair |
| `RegisterNode` | Storage node | Register or re-register storage node |
| `DeregisterNode` | Storage node | Mark node draining, trigger repair |
| `Heartbeat` | Storage node | Update last seen, return piggybacked repair jobs |
| `ReportRepairResult` | Storage node | Report repair job outcome |

### StorageService (`proto/storage/v1/storage.proto`)

| Method | Type | Caller | Description |
|---|---|---|---|
| `PutChunk` | Client streaming | Client | Upload a chunk to primary node |
| `GetChunk` | Server streaming | Client | Download a chunk |
| `DeleteChunk` | Unary | Metadata | Evict a chunk |
| `VerifyChunk` | Unary | Metadata | Check chunk integrity |

### ReplicationService (in `proto/storage/v1/storage.proto`)

| Method | Type | Caller | Description |
|---|---|---|---|
| `ReplicateChunk` | Bidirectional streaming | Storage node | P2P chunk replication |

---

## File Upload Flow

```
1. Client splits file into 4MB chunks
2. Client generates file_id (UUID), computes chunk_ids as SHA256(file_id + chunk_index)
3. Client writes local manifest
4. Client → MetadataService.CreateFile → receives placement per chunk (primary + replicas)
5. Client → StorageService.PutChunk (to primary node for each chunk, in parallel)
6. Primary writes locally, fans out P2P replication to replicas
7. Primary → MetadataService.CommitChunk (after replication quorum)
8. Client → MetadataService.CommitFile (after all chunks committed)
9. Client deletes manifest
```

## File Download Flow

```
1. Client → MetadataService.GetFile → receives ordered chunk list with live node addresses
2. Client → StorageService.GetChunk (in parallel from any live replica per chunk)
3. Client verifies SHA256 checksum per chunk
4. Client reassembles file in chunk_index order
```

## Failure Detection and Repair

```
1. Storage nodes send heartbeat every 3s to metadata
2. Metadata marks node Suspect after 10s, Dead after 30s
3. Dead node triggers repair: metadata scans chunks on dead node, creates repair jobs
4. Repair jobs piggybacked on heartbeat responses to source nodes
5. Source node replicates chunk to new target node via P2P
6. If node restarts after repair, stale replicas evicted during reconciliation
```

---

## Consistency Model

**Write Once — Read Many**

- No concurrent writes
- No distributed locking
- Simplifies replication
- Safe for academic DFS

---

## Chunk Layout

- Chunk size: 4 MB default (configurable 64 KB – 64 MB)
- Chunk ID: `SHA256(file_id + chunk_index)` — deterministic, known before upload
- Two-level directory sharding: `xx/yy/chunk_id.chunk`
- Atomic write: tmp file → fsync → atomic rename
- Checksums in BoltDB index

---

## Placement Strategy

- Select nodes with most free space
- Exclude nodes already holding the chunk
- Avoid duplicate replica on same node

---

## Fault Tolerance

### Metadata Layer
- Raft majority commit (3-5 nodes)
- Automatic leader election
- Log replication and snapshots

### Storage Layer
- Replication factor = 3 (configurable)
- Automatic repair on node death
- SHA256 checksum verification
- Atomic writes (no partial chunks)

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
| Chunk storage | Disk (two-level sharded directories) |

---

## Configuration

Components are configured via environment variables or JSON config files. See:
- [storage.md](storage.md) — Storage node config
- [metadata.md](metadata.md) — Metadata node config
- [client.md](client.md) — Client config
- [configuration.md](configuration.md) — How everything links together

---

## Differences from GFS

- Simplified write pipeline
- No lease-based write ordering
- No rack-awareness
- Raft instead of custom master journal
- Write-once model (no appends)

---

## Differences from BitTorrent

- Managed storage nodes with identities
- Consensus metadata (not tracker-based)
- Guaranteed replication factor
- Persistent storage responsibility
- Controlled placement strategy

---

## Related Docs

- [Storage Node Implementation](Storage%20Node%20Implementation.md) — Detailed design
- [Client Implementation](Client%20Implementation.md) — Detailed design
- [Metadata Node Implementation Plan](Metadata%20Node%20Implementation%20Plan.md) — Detailed design
- [Integration Testing Guide](integration_testing_guide.md) — Test approach
- [Project Structure](project-structure.md) — File layout
