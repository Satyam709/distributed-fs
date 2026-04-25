# Metadata Node Implementation Plan

---

## Component Map

```
MetadataNode
  ├── RaftNode                    (hashicorp/raft setup and lifecycle)
  ├── MetadataFSM                 (your state machine — the core)
  │     ├── FileIndex             (file_id → file record)
  │     ├── ChunkRegistry         (chunk_id → replica nodes)
  │     ├── NodeRegistry          (node_id → node info + status)
  │     └── RepairJobRegistry     (job_id → repair job)
  ├── BoltStore                   (Raft log + stable store persistence)
  ├── NodeWatcher                 (heartbeat timeout detection)
  ├── RepairScheduler             (repair job creation and dispatch)
  ├── PlacementStrategy           (which nodes get new chunks)
  ├── MetadataServiceHandler      (gRPC — clients + storage nodes)
  └── config.go
```

---

## 1. Config

Single struct read at startup. Every tunable lives here — nothing hardcoded anywhere else.

**Fields needed:**

- `NodeID` — unique ID for this metadata node in the Raft cluster
- `GRPCAddr` — address to serve gRPC on (`:9000`)
- `RaftAddr` — address for Raft internal communication (`:9001`)
- `RaftDir` — directory for Raft log, snapshots, stable store
- `PeerAddrs` — map of other metadata node IDs to their Raft addresses (for bootstrapping)
- `ReplicationFactor` — target replica count per chunk (default 3)
- `SuspectTimeout` — how long before a node is marked suspect (default 9s)
- `DeadTimeout` — how long before a node is marked dead (default 15s)
- `WatcherInterval` — how often NodeWatcher sweeps (default 5s)
- `ReconcileDelay` — delay before reconciling a rejoining node (default 30s)
- `SnapshotThreshold` — how many log entries before Raft triggers snapshot (default 1000)
- `SnapshotRetain` — how many snapshots to keep on disk (default 2)

---

## 2. Core Data Types

Define these first. Everything else references them.

### FileRecord

Represents a file in the system.

**Fields:** `FileID`, `Filename`, `FileSize`, `ChunkSize`, `ChunkIDs []string` (ordered — index = chunk_index), `Status` (creating/complete/deleted), `CreatedAt`, `UpdatedAt`

### ChunkRecord

Represents a single chunk and where it lives.

**Fields:** `ChunkID`, `FileID`, `ChunkIndex`, `Size`, `Checksum`, `Replicas []string` (node IDs), `Status` (allocated/complete/lost), `Version`

### NodeEntry

Represents a storage node as metadata knows it.

**Fields:** `NodeID`, `Address`, `Status` (alive/suspect/dead/draining), `FreeSpace`, `ChunkCount`, `LastSeen` (in-memory only, not persisted through Raft), `RegisteredAt`

### RepairJob

Represents one unit of repair work.

**Fields:** `JobID`, `ChunkID`, `SourceNodeID`, `TargetNodeID`, `Status` (pending/in-progress/done/failed), `Attempts`, `CreatedAt`, `UpdatedAt`

---

## 3. MetadataFSM — The Heart of Everything

This is what you build. hashicorp/raft calls into this. It holds all system state in memory, replicated across the cluster.

### What It Must Implement (raft.FSM interface)

**`Apply(log *raft.Log) interface{}`**

Called by Raft on every committed log entry. This is the only place state is mutated. Takes a raw log entry, deserializes it into a `MetadataCommand`, switches on command type, updates the appropriate in-memory map.

Must be deterministic — given the same sequence of commands, every FSM replica must produce identical state. No randomness, no timestamps generated inside Apply (timestamps must come in the command itself).

**`Snapshot() (raft.FSMSnapshot, error)`**

Called by Raft when it wants to compact the log. Must serialize the entire current FSM state into a snapshot. Returns an `FSMSnapshot` object.

Takes a read lock on the FSM, serializes all four registries (FileIndex, ChunkRegistry, NodeRegistry, RepairJobRegistry) into JSON or protobuf bytes. Must not block Apply — take a copy under lock, release lock, then serialize the copy.

**`Restore(rc io.ReadCloser) error`**

Called by Raft on startup (if snapshot exists) or when a lagging follower needs to catch up. Must completely replace current FSM state with what's in the snapshot. Deserializes the snapshot bytes back into the four registries.

### FSMSnapshot Interface

**`Persist(sink raft.SnapshotSink) error`**

Writes the serialized snapshot bytes into the sink. Called after Snapshot() returns.

**`Release()`**

Cleanup after snapshot is done. Usually empty.

---

### MetadataCommand — Every State Change Goes Through This

All mutations are expressed as commands. Commands are serialized to JSON, appended to the Raft log, and replayed through Apply.

**Command types:**

| Command | Triggered By | What Apply Does |
| --- | --- | --- |
| `CmdRegisterNode` | Storage node startup | Add NodeEntry to NodeRegistry |
| `CmdDeregisterNode` | Storage node graceful shutdown | Mark node draining |
| `CmdMarkNodeDead` | NodeWatcher timeout | Mark node dead, remove from placement eligibility |
| `CmdMarkNodeAlive` | Node re-registration | Update node status to alive |
| `CmdUpdateNodeSpace` | Periodic sync from heartbeat | Update FreeSpace + ChunkCount on NodeEntry |
| `CmdCreateFile` | Client upload start | Add FileRecord (status: creating), record chunk_id list |
| `CmdCommitFile` | Client after all chunks done | Mark FileRecord status complete |
| `CmdDeleteFile` | Client delete request | Mark FileRecord deleted, enqueue eviction for all chunk replicas |
| `CmdCommitChunk` | Storage node after replication | Update ChunkRecord replicas to confirmed list, mark complete |
| `CmdEvictChunkFromNode` | Reconciliation, node death | Remove node from ChunkRecord replica list |
| `CmdMarkChunkLost` | Repair exhausted, no replicas | Mark ChunkRecord status lost |
| `CmdCreateRepairJob` | RepairScheduler | Add RepairJob to RepairJobRegistry |
| `CmdUpdateRepairJob` | Storage node repair result | Update RepairJob status |

Each command carries all data needed for Apply to execute — no side effects, no external calls inside Apply ever.

---

### FSM Read Methods (not through Raft — direct in-memory reads)

These are called by the gRPC handler to serve reads. They take a read lock.

**File reads:**

- `GetFile(fileID string) (*FileRecord, error)` — lookup by ID
- `GetFileByName(filename string) (*FileRecord, error)` — lookup by name
- `ListFiles(prefix string) ([]*FileRecord, error)` — scan with optional filter
- `GetFileChunks(fileID string) ([]*ChunkRecord, error)` — all chunks for a file in order

**Chunk reads:**

- `GetChunk(chunkID string) (*ChunkRecord, error)`
- `GetChunkLocations(chunkID string) ([]NodeEntry, error)` — returns live nodes only, filters dead
- `GetChunksByNode(nodeID string) ([]string, error)` — all chunk IDs on a given node

**Node reads:**

- `GetNode(nodeID string) (*NodeEntry, error)`
- `GetLiveNodes() ([]NodeEntry, error)` — only alive nodes
- `GetNodeCount() int`

**Repair reads:**

- `GetRepairJob(jobID string) (*RepairJob, error)`
- `GetJobsByStatus(status RepairStatus) ([]*RepairJob, error)` — used for crash recovery
- `GetPendingJobsForNode(nodeID string) ([]*RepairJob, error)` — for heartbeat piggyback

---

### FSM Write Helper — Propose to Raft

All writes go through one helper method on the FSM (or on the MetadataNode):

**`propose(cmd MetadataCommand) error`**

Objective: serialize the command to bytes, call `raft.Apply(bytes, timeout)`, wait for commit, return error if not leader or if commit fails. This is the single chokepoint for all state changes. Every component that needs to mutate state calls this.

---

## 4. BoltStore — Raft Persistence

hashicorp/raft needs two stores for persistence:

- **LogStore** — stores the Raft log entries
- **StableStore** — stores Raft metadata (current term, last vote)

hashicorp/raft-boltdb provides both in one BoltDB file. You just initialize it and pass it to Raft — no methods to implement yourself.

**`NewBoltStore(path string) (*raftboltdb.BoltStore, error)`**

Objective: open or create BoltDB file at `path`, return a store that implements both `raft.LogStore` and `raft.StableStore`. Pass both interfaces to the Raft configuration.

One file: `<RaftDir>/raft.db`

---

## 5. RaftNode — Raft Setup and Lifecycle

Wraps hashicorp/raft initialization. Not a long file — mostly configuration and wiring.

**`NewRaftNode(config NodeConfig, fsm raft.FSM, logStore, stableStore) (*raft.Raft, error)`**

Objective: build the `raft.Config`, set up TCP transport on `RaftAddr`, initialize the Raft instance with FSM + stores, bootstrap single-node or join existing cluster based on whether peers are configured.

Key config fields to set:

- `HeartbeatTimeout` — how often Raft leader sends heartbeats to followers
- `ElectionTimeout` — how long before a follower starts an election
- `SnapshotInterval` — how often to check if snapshot is needed
- `SnapshotThreshold` — log entries since last snapshot before triggering new one
- `LogOutput` — wire to your logger

**`isLeader() bool`**

Objective: return `raft.State() == raft.Leader`. Used by gRPC handler to reject writes when not leader.

**`leaderAddress() string`**

Objective: return current leader's address from `raft.Leader()`. Used by handler to tell clients where to redirect.

**`WaitForLeader(timeout time.Duration) error`**

Objective: block until this node observes a leader or timeout expires. Called during startup so the node is ready before serving requests.

---

## 6. NodeWatcher — Heartbeat Timeout Detection

Runs on the leader only. Sweeps NodeRegistry periodically and detects dead nodes.

**`NewNodeWatcher(fsm *MetadataFSM, scheduler *RepairScheduler, config NodeConfig)`**

**`Start(ctx context.Context)`**

Objective: start a goroutine with a ticker at `WatcherInterval`. On each tick, call `sweep()`. Stop when context is cancelled. Should only do meaningful work when this node is the Raft leader — check `isLeader()` at the top of each sweep.

**`sweep()`**

Objective: iterate all nodes in NodeRegistry. For each node:

- If `Alive` and `now - LastSeen > SuspectTimeout` → propose `CmdMarkNodeDead` (skip suspect state for simplicity — go straight to dead)
- If `Dead` and `now - LastSeen < SuspectTimeout` → node may have recovered, reconciliation handles this

Note: `LastSeen` is updated in memory directly by the heartbeat handler — NOT through Raft. Too frequent for the log. Only the death/recovery decision goes through Raft.

**`UpdateLastSeen(nodeID string, timestamp time.Time)`**

Objective: directly update the `LastSeen` field on the NodeEntry in memory. Called by the gRPC heartbeat handler. Not a Raft operation — purely in-memory.

---

## 7. RepairScheduler — Repair Job Creation and Dispatch

Runs on the leader only. Creates repair jobs when under-replication is detected, dispatches them to source nodes.

**`NewRepairScheduler(fsm *MetadataFSM, placement *PlacementStrategy, config NodeConfig)`**

**`Start(ctx context.Context)`**

Objective: start repair worker goroutines consuming an internal job channel. Also start a recovery goroutine that runs once on startup to recover stuck in-progress jobs.

**`TriggerRepair(deadNodeID string)`**

Objective: called by NodeWatcher when a node is marked dead. Scans all chunks that had a replica on the dead node via `fsm.GetChunksByNode(deadNodeID)`. For each chunk, computes current live replica count. If below replication factor, calculates deficit and calls `scheduleRepairJobs` for each missing replica.

**`scheduleRepairJobs(chunkID string, liveReplicas []NodeEntry, deficit int)`**

Objective: for each missing replica, pick a source node (live replica with lowest load) and a target node (PlacementStrategy — most free space, not already holding this chunk). Propose `CmdCreateRepairJob` through Raft. Enqueue to internal job channel.

**`executeJob(job RepairJob)`**

Objective: deliver the repair instruction to the source node. Repair instructions are piggybacked on heartbeat responses — so this method adds the job to a pending map keyed by source node ID. When that node next heartbeats, the handler includes the pending job in the response.

**`RecoverStuckJobs()`**

Objective: called once on leader startup or after election. Scans all jobs with status `InProgress` via `fsm.GetJobsByStatus(InProgress)`. For each, checks if source node is still alive. If not, resets job to pending and re-enqueues with a new source node.

**`OnJobComplete(jobID string, success bool, error string)`**

Objective: called by gRPC handler when a storage node reports repair outcome. Proposes `CmdUpdateRepairJob` with Done or Failed status. If failed and chunk still under-replicated, re-schedules.

---

## 8. PlacementStrategy — Which Nodes Get Chunks

Called during `CreateFile` (initial placement) and `scheduleRepairJobs` (repair placement).

**`SelectNodes(chunkID string, count int, exclude []string) ([]NodeEntry, error)`**

Objective: from all live nodes, exclude any node already holding this chunk (passed in exclude list), sort remaining by free space descending, return top `count` nodes. If fewer than `count` live nodes available, return error with how many were found — caller decides if partial placement is acceptable.

**`SelectPrimary(nodes []NodeEntry) NodeEntry`**

Objective: from a list of selected nodes, pick one as primary. Simple rule — node with most free space or lowest active stream count.

**`SelectRepairTarget(chunkID string, exclude []string) (*NodeEntry, error)`**

Objective: same as SelectNodes but for a single target. Used by repair scheduler.

---

## 9. Reconciliation — Handling Node Restarts

**`ReconcileNode(nodeID string, reportedChunks []string)`**

Objective: called 30 seconds after a node registers (delayed via `time.AfterFunc`). Compares what the node reported having vs what the FSM's ChunkRegistry says it should have.

Two cases to handle:

- Node has chunk but not in canonical replica list → propose `CmdEvictChunkFromNode`, send `DeleteChunk` RPC to storage node
- FSM expected chunk on node but node doesn't have it → propose `CmdEvictChunkFromNode` to clean FSM record, call `TriggerRepair` if now under-replicated

This is the only place metadata acts as a gRPC client to storage nodes (for eviction). Keep a thin StorageClient in the metadata node for this purpose.

---

## 10. MetadataServiceHandler — gRPC Layer

Implements the MetadataService proto. All methods check `isLeader()` first — if not leader, return a gRPC error with the leader address so the client can redirect.

### File Methods

**`CreateFile(ctx, req) → placement response`**

Objective: validate no duplicate file_id. For each chunk_id in the request, call `PlacementStrategy.SelectNodes` to get primary + replicas. Propose `CmdCreateFile` through Raft. Return placement map to client.

**`CommitFile(ctx, req) → ack`**

Objective: propose `CmdCommitFile`. Validates all chunks for this file are in complete status before committing — if any chunk is still in allocated state, return error.

**`GetFile(ctx, req) → file response`**

Objective: read from FSM (no Raft — direct memory read). Call `fsm.GetFile` + `fsm.GetFileChunks`. For each chunk, call `fsm.GetChunkLocations` to get live nodes only. Assemble response.

**`DeleteFile(ctx, req) → ack`**

Objective: propose `CmdDeleteFile`. For each chunk replica, enqueue eviction — add to RepairScheduler's eviction queue which sends `DeleteChunk` RPC to storage nodes asynchronously.

**`ListFiles(ctx, req) → file list`**

Objective: direct FSM read. Call `fsm.ListFiles`. No Raft.

### Chunk Methods

**`CommitChunk(ctx, req) → ack`**

Objective: propose `CmdCommitChunk` with confirmed node list and checksum. If confirmed replica count is below replication factor, immediately schedule repair jobs for the deficit.

**`GetChunkLocations(ctx, req) → node list`**

Objective: direct FSM read. Filter dead nodes. Return live addresses only.

**`ReportCorruption(ctx, req) → ack`**

Objective: propose `CmdEvictChunkFromNode` to remove reporting node from replica list. If now under-replicated, trigger repair.

### Node Methods

**`RegisterNode(ctx, req) → ack`**

Objective: propose `CmdRegisterNode`. Schedule `ReconcileNode` to run after `ReconcileDelay` via `time.AfterFunc`. Return confirmed node_id.

**`DeregisterNode(ctx, req) → ack`**

Objective: propose `CmdDeregisterNode` (status: draining). Trigger repair for all chunks on this node immediately.

**`Heartbeat(ctx, req) → response with optional repair instructions`**

Objective:

1. Call `NodeWatcher.UpdateLastSeen(nodeID, now)` — in memory, not Raft
2. Periodically (not every heartbeat) propose `CmdUpdateNodeSpace` with fresh free_space + chunk_count — maybe every 10th heartbeat to avoid log spam
3. Check RepairScheduler's pending jobs for this node
4. Return any pending repair instructions in the response

### Repair Methods

**`ReportRepairFailure(ctx, req) → ack`**

Objective: call `RepairScheduler.OnJobComplete(jobID, false, error)`. Scheduler decides whether to retry with a different source or mark the chunk as permanently under-replicated.

---

## 11. MetadataNode — Root Struct

Wires everything together. Startup and shutdown sequences.

**`NewMetadataNode(config NodeConfig) (*MetadataNode, error)`**

Objective: initialize components in dependency order — BoltStore first, then FSM, then Raft, then NodeWatcher and RepairScheduler, then gRPC server last.

**`Start(ctx context.Context) error`**

Startup sequence:

1. Open BoltStore
2. Initialize FSM (empty state)
3. Initialize RaftNode — if existing log present, Raft restores FSM via Snapshot+Apply automatically
4. Call `WaitForLeader` — block until cluster has a leader
5. If this node becomes leader, call `RepairScheduler.RecoverStuckJobs`
6. Start NodeWatcher
7. Start RepairScheduler workers
8. Start gRPC server
9. Register leadership change callback — when leadership changes hands, start/stop NodeWatcher and RepairScheduler accordingly

**`Shutdown(ctx context.Context) error`**

Shutdown sequence:

1. Stop gRPC server gracefully
2. Stop NodeWatcher and RepairScheduler
3. Snapshot FSM state before shutdown (optional but good practice)
4. Shutdown Raft
5. Close BoltStore

**Leadership change callback**

Objective: hashicorp/raft provides a `LeaderCh()` channel that emits when leadership changes. Watch this channel in a goroutine. On becoming leader: start NodeWatcher, start RepairScheduler, run RecoverStuckJobs. On losing leadership: stop NodeWatcher, stop RepairScheduler (followers do nothing — they just apply log entries).

---

## 12. Raft Elements — What The Library Does vs What You Do

### Library handles completely (you write zero code for these):

- Leader election algorithm
- Log replication to followers
- Majority quorum acknowledgment
- Network transport between Raft peers
- Snapshot triggering based on threshold
- Sending snapshots to lagging followers
- Log compaction after snapshot
- Term management and vote tracking
- Stepdown when leader loses quorum

### You implement:

- `Apply()` — interpret every log entry, mutate FSM state
- `Snapshot()` — serialize FSM state when Raft asks
- `Restore()` — deserialize FSM state from snapshot
- `Persist()` — write snapshot bytes to Raft's sink
- Leader redirect in gRPC handler — library tells you who leader is, you forward the address to clients
- Leadership change reaction — watch `LeaderCh()`, start/stop NodeWatcher and RepairScheduler
- Bootstrap logic — first cluster startup vs joining existing cluster

### The propose → apply lifecycle (important to understand):

```
gRPC handler receives request
    │
    ▼
propose(cmd) called
    │
    ▼
cmd serialized to bytes
    │
    ▼
raft.Apply(bytes, timeout) called — blocks
    │
    ▼
Raft replicates to followers
    │
    ▼
Majority ack received
    │
    ▼
Raft calls FSM.Apply(log) on leader AND followers
    │
    ▼
Your Apply() deserializes cmd, mutates in-memory state
    │
    ▼
raft.Apply() returns to propose()
    │
    ▼
propose() returns to gRPC handler
    │
    ▼
handler returns response to client
```

The client only gets a response after the commit is durable on a majority. That's the consistency guarantee.

---

## Build Order

```
Week 2 Day 1 — together
  Define all data types (FileRecord, ChunkRecord, NodeEntry, RepairJob)
  Define all MetadataCommand types
  Define FSM interface and propose helper

Day 2
  Person A: MetadataFSM — Apply() with all command types, read methods
  Person B: BoltStore setup + RaftNode initialization + config

Day 3
  Person A: FSM Snapshot, Restore, Persist
  Person B: MetadataServiceHandler skeleton — all methods, leader redirect

Day 4
  Person A: NodeWatcher — sweep, UpdateLastSeen, leadership awareness
  Person B: PlacementStrategy — SelectNodes, SelectPrimary, SelectRepairTarget

Day 5
  Person A: RepairScheduler — TriggerRepair, scheduleRepairJobs, RecoverStuckJobs
  Person B: Reconciliation logic + thin StorageClient for eviction RPCs

Day 6-7 — together
  MetadataNode root struct wiring
  Leadership change callback
  Integration — storage nodes registering, heartbeating, uploading chunks
  End to end test: upload file → kill storage node → verify repair triggers
```

---

## Key Invariants to Maintain

These must hold at all times or the system breaks:

- **Only leader proposes commands.** Followers only apply. Never call `propose()` on a follower.
- **Apply() never fails.** If a command is in the log it must be applied. Validation happens before propose, never inside Apply.
- **LastSeen never goes through Raft.** Heartbeats are too frequent. Only the death decision is committed.
- **Canonical replica list is truth.** Storage nodes conform to FSM state, never the other way around.
- **Repair jobs survive leader failover.** Because they're in the FSM via Raft, the new leader finds them via RecoverStuckJobs.
- **Apply() is deterministic.** No `time.Now()` inside Apply — timestamps come in the command. No random inside Apply.