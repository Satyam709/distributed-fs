# Metadata Node — Raft-Based Metadata Cluster

## Overview

The Metadata Node is the control plane of the distributed file system. A cluster of metadata nodes uses the **Raft consensus algorithm** (hashicorp/raft) to maintain a strongly consistent, replicated state machine. The metadata cluster tracks every file, chunk placement, registered storage node, and in-flight repair job in the system.

Only the Raft leader handles write operations. Followers redirect clients to the leader. All non-write reads can be served locally from the in-memory FSM.

---

## Configuration

### Config Loading Priority

1. **Defaults** — struct defaults applied first
2. **JSON file** — loaded if `METADATA_CONFIG` env var points to a valid path
3. **Environment variables** — `METADATA_*` prefixed vars override corresponding fields
4. **Validate** — configuration is validated for correctness

### Environment Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `METADATA_NODE_ID` | string | `node-1` | Unique node identifier |
| `METADATA_GRPC_ADDR` | string | `:4001` | gRPC listen address for clients and storage nodes |
| `METADATA_RAFT_ADDR` | string | `127.0.0.1:5001` | Raft internal communication address |
| `METADATA_RAFT_ADVERTISE` | string | — | Raft address advertised to peers (Docker/NAT) |
| `METADATA_RAFT_DIR` | string | `./data/metadata/raft` | Raft log and snapshot directory |
| `METADATA_PEER_ADDRS` | string | — | Comma-separated `nodeID:addr` pairs (e.g., `node-2:node-2:5002,node-3:node-3:5003`) |
| `METADATA_BOOTSTRAP` | bool | `false` | `true` for the first node in a new cluster |
| `METADATA_REPLICATION_FACTOR` | int | `3` | Target replica count per chunk |
| `METADATA_SUSPECT_TIMEOUT` | duration | `10s` | Time before a node is marked suspect |
| `METADATA_DEAD_TIMEOUT` | duration | `30s` | Time before a node is marked dead |
| `METADATA_WATCHER_INTERVAL` | duration | `5s` | NodeWatcher sweep frequency |
| `METADATA_RECONCILE_DELAY` | duration | `30s` | Delay before reconciling a rejoining node |
| `METADATA_HEARTBEAT_TIMEOUT` | duration | `1s` | Raft heartbeat interval |
| `METADATA_ELECTION_TIMEOUT` | duration | `5s` | Raft election timeout |
| `METADATA_SNAPSHOT_INTERVAL` | duration | `30s` | Snapshot check interval |
| `METADATA_SNAPSHOT_THRESHOLD` | uint64 | `8192` | Log entries before triggering snapshot |
| `METADATA_SNAPSHOT_RETAIN` | int | `2` | Snapshots to retain on disk |
| `METADATA_CONFIG` | string | — | Path to JSON config file |

### JSON Config Example

```json
{
  "node_id": "node-1",
  "grpc_addr": ":4001",
  "raft_addr": "127.0.0.1:5001",
  "raft_advertise": "node-1:5001",
  "raft_dir": "./data/metadata/raft",
  "peer_addrs": {
    "node-2": "node-2:5002",
    "node-3": "node-3:5003"
  },
  "bootstrap": true,
  "replication_factor": 3,
  "suspect_timeout": "10s",
  "dead_timeout": "30s",
  "watcher_interval": "5s",
  "reconcile_delay": "30s",
  "heartbeat_timeout": "1s",
  "election_timeout": "5s",
  "snapshot_interval": "30s",
  "snapshot_threshold": 8192,
  "snapshot_retain": 2
}
```

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                     MetadataApp (app.go)                    │
│  Orchestrates lifecycle: Start → serve → Stop              │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────┐   ┌──────────────┐   ┌───────────────────┐  │
│  │ RaftNode │   │ MetadataFSM  │   │ NodeWatcher       │  │
│  │(raft.go) │◄──│ (fsm/fsm.go) │   │ (watcher/)        │  │
│  │          │   │              │   │ leader-only sweep │  │
│  │hashicorp │   │ FileIndex    │   └────────┬──────────┘  │
│  │ /raft    │   │ ChunkRegistry│            │             │
│  └────┬─────┘   │ NodeRegistry │   ┌────────▼──────────┐  │
│       │         │ RepairJobs   │   │ RepairScheduler   │  │
│       │         └──────┬───────┘   │ (scheduler/)      │  │
│       │                │           │ leader-only       │  │
│  ┌────▼─────┐   ┌──────▼───────┐   │ creates repair    │  │
│  │ BoltStore│   │ Placement    │   │ jobs for under-   │  │
│  │(store/)  │   │ Strategy     │   │ replicated chunks │  │
│  │BoltDB    │   │(placement/)  │   └────────┬──────────┘  │
│  │log+stable│   │ SelectNodes  │            │             │
│  └──────────┘   └──────────────┘   ┌────────▼──────────┐  │
│                                    │ Reconciler        │  │
│  ┌──────────────────────────────┐  │ (reconciler/)     │  │
│  │        gRPC Server           │  │ handles node      │  │
│  │  (server/server.go)          │  │ restarts + stale  │  │
│  │  File / Chunk / Node /       │  │ replica eviction  │  │
│  │  Repair handlers             │  └───────────────────┘  │
│  └──────────────────────────────┘                          │
└────────────────────────────────────────────────────────────┘
```

### External Connections

```
Clients ──────► gRPC :4001 (CreateFile, GetFile, ListFiles, etc.)
Storage Nodes ► gRPC :4001 (RegisterNode, Heartbeat, CommitChunk, ReportRepairResult)
Peers ◄───────► Raft :5001 (log replication, leader election)
```

---

## FSM — Finite State Machine

The Metadata FSM (`fsm/fsm.go`) is the Raft state machine. Every mutation goes through `MetadataFSM.Apply(log)` on every node in the cluster.

### State Registers

| Register | Stores |
|---|---|
| FileIndex | file_id → FileRecord (name, size, chunk list, status) |
| ChunkRegistry | chunk_id → ChunkRecord (size, checksum, replica nodes) |
| NodeRegistry | node_id → NodeEntry (address, status, free space, last seen) |
| RepairJobRegistry | job_id → RepairJob (chunk_id, source, target, status) |

### Key Invariants

- Only leader proposes commands to Raft (followers only apply)
- `Apply()` never fails — validation happens before propose
- `LastSeen` never goes through Raft (too frequent — only death decisions are committed)
- Canonical replica list in FSM is source of truth; nodes conform to it
- `Apply()` is deterministic — no `time.Now()` or random inside

---

## Startup Sequence

1. Load configuration (defaults → JSON → env vars → validate)
2. Create Raft transport and BoltDB log/stable stores
3. If `Bootstrap == true`: create cluster with single voter (this node)
4. If joining: contact peers and request voting membership
5. Start Raft node — leader election if needed
6. Apply any committed-but-unapplied logs to FSM
7. Start gRPC server on `GRPCAddr`
8. If leader: start NodeWatcher goroutine, start RepairScheduler goroutine, run `RecoverStuckJobs()`
9. Node ready for client requests

---

## Leader Election and Failover

| Event | Behavior |
|---|---|
| Leader heartbeat | Sent every `HeartbeatTimeout` (1s) to followers |
| Follower timeout | Starts election after `ElectionTimeout` (5s) |
| Leader failure | New leader elected within ~5-10s |
| Client redirect | Follower returns leader address in gRPC error detail |
| Watcher/Scheduler | Run leader-only; stopped on leadership loss |

The `raft.LeaderCh()` channel is watched in a goroutine to detect leadership changes.

---

## Failure Detection — NodeWatcher

Runs on the Raft leader only. Sweeps the NodeRegistry at `WatcherInterval` (default 5s):

- **Alive → Suspect**: `now - LastSeen > SuspectTimeout` (10s)
- **Suspect → Dead**: `now - LastSeen > DeadTimeout` (30s)
- **Dead → triggers repair**: all chunks on the dead node are scanned; under-replicated chunks get repair jobs

---

## Repair Scheduling

Runs on the Raft leader only. Triggered when a node is marked dead.

1. Scan all chunks that had a replica on the dead node
2. For each chunk below `ReplicationFactor`, pick a source node (live replica) and a target node (PlacementStrategy — most free space)
3. Create RepairJob via Raft command
4. Piggyback repair instruction on next heartbeat response to the source node
5. Source node replicates chunk to target node via P2P
6. Source reports result; metadata updates job status

### Piggyback Delivery

Repair jobs are piggybacked on heartbeat responses — metadata never opens connections to storage nodes for routine work.

---

## Reconciliation

When a storage node restarts and re-registers:

1. Wait `ReconcileDelay` (30s) to confirm stability
2. Compare node's reported chunk inventory against FSM's canonical replica list
3. **Stale replica**: node has chunk but not in canonical list → eviction RPC to node, remove from FSM
4. **Missing chunk**: FSM expected chunk on node but node doesn't have it → remove from record, trigger repair if under-replicated

The FSM is always the source of truth.

---

## gRPC Service Methods (MetadataService)

| Method | Caller | Description |
|---|---|---|
| `CreateFile` | Client | Create file with chunk list, return placements per chunk |
| `GetFile` | Client | Return file metadata + chunk list with live node addresses |
| `DeleteFile` | Client | Delete file, enqueue eviction for all chunk replicas |
| `ListFiles` | Client | List files with optional prefix filter |
| `CommitFile` | Client | Mark file as complete |
| `CommitChunk` | Storage node | Update chunk with confirmed replica nodes |
| `GetChunkLocations` | Client | Return live node addresses for a chunk |
| `ReportCorruption` | Storage node | Remove node from replica list, trigger repair |
| `RegisterNode` | Storage node | Register or re-register node with inventory |
| `DeregisterNode` | Storage node | Mark node draining, trigger repair for its chunks |
| `Heartbeat` | Storage node | Update last seen time; return piggybacked repair jobs |
| `ReportRepairResult` | Storage node | Report repair job outcome |

---

## Multi-Node Cluster Setup

### Node 1 (bootstrap)

```bash
export METADATA_NODE_ID=node-1
export METADATA_GRPC_ADDR=:4001
export METADATA_RAFT_ADDR=0.0.0.0:5001
export METADATA_RAFT_ADVERTISE=node-1:5001
export METADATA_RAFT_DIR=./data/metadata/raft
export METADATA_PEER_ADDRS="node-2:node-2:5002,node-3:node-3:5003"
export METADATA_BOOTSTRAP=true
export METADATA_REPLICATION_FACTOR=3
```

### Node 2

```bash
export METADATA_NODE_ID=node-2
export METADATA_GRPC_ADDR=:4002
export METADATA_RAFT_ADDR=0.0.0.0:5002
export METADATA_RAFT_ADVERTISE=node-2:5002
export METADATA_RAFT_DIR=./data/metadata/raft
export METADATA_PEER_ADDRS="node-1:node-1:5001,node-3:node-3:5003"
export METADATA_BOOTSTRAP=false
```

### Node 3

```bash
export METADATA_NODE_ID=node-3
export METADATA_GRPC_ADDR=:4003
export METADATA_RAFT_ADDR=0.0.0.0:5003
export METADATA_RAFT_ADVERTISE=node-3:5003
export METADATA_RAFT_DIR=./data/metadata/raft
export METADATA_PEER_ADDRS="node-1:node-1:5001,node-2:node-2:5002"
export METADATA_BOOTSTRAP=false
```

**Start order:** node-1 first (bootstrap, forms cluster solo), then node-2 and node-3 (join as voters, sync logs).

---

## Package Structure

| Package | Path | Responsibility |
|---|---|---|
| `main` | `metadata/cmd/main.go` | Entry point |
| `metadata` | `metadata/app.go` | `MetadataApp` root struct, Start/Stop |
| `metadata` | `metadata/config.go` | `NodeConfig` and loading |
| `metadata` | `metadata/raft.go` | `RaftNode` wrapper around hashicorp/raft |
| `fsm` | `metadata/fsm/fsm.go` | `MetadataFSM` — Apply, Snapshot, Restore |
| `fsm` | `metadata/fsm/commands.go` | All `MetadataCommand` types |
| `fsm` | `metadata/fsm/types.go` | `FileRecord`, `ChunkRecord`, `NodeEntry`, `RepairJob` |
| `placement` | `metadata/placement/strategy.go` | Node selection for chunk placement |
| `reconciler` | `metadata/reconciler/reconciler.go` | Node restart reconciliation |
| `scheduler` | `metadata/scheduler/repair_scheduler.go` | Repair job creation and dispatch |
| `server` | `metadata/server/server.go` | gRPC server setup |
| `server` | `metadata/server/handler.go` | Main handler with leader check |
| `server` | `metadata/server/file_handler.go` | File operations |
| `server` | `metadata/server/chunk_handlers.go` | Chunk operations |
| `server` | `metadata/server/node_handlers.go` | Node registration/heartbeat |
| `server` | `metadata/server/repair_handlers.go` | Repair reporting |
| `store` | `metadata/store/boltstore.go` | BoltDB Raft log/stable store |
| `watcher` | `metadata/watcher/node_watcher.go` | Heartbeat timeout detection |
