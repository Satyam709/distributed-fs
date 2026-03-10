# 📄 Distributed File System in Go

---

# Fault-Tolerant Distributed File System

## Overview

This project implements a fault-tolerant distributed file system with:

- Replicated metadata using Raft consensus
- Peer-to-peer chunk replication between storage nodes
- gRPC-based inter-service communication
- Client-side parallel chunk transfer
- Automatic failure detection and re-replication
- Chunk integrity verification via checksums

The system separates:

- Control Plane → Metadata cluster
- Data Plane → Storage nodes

This architecture is inspired by GFS/HDFS but simplified for academic implementation.

---

# High Level Architecture

```mermaid
flowchart TB

Client[Client CLI]

subgraph META["Metadata Cluster (Raft)"]
M1[Meta Node A - Leader]
M2[Meta Node B - Follower]
M3[Meta Node C - Follower]
M1 --- M2
M2 --- M3
M1 --- M3
end

subgraph STORAGE["Storage Node Network"]
S1[Storage Node 1]
S2[Storage Node 2]
S3[Storage Node 3]
S4[Storage Node 4]
S1 --- S2
S2 --- S3
S3 --- S4
S1 --- S4
end

Client -->|Metadata gRPC| M1
Client -->|Chunk gRPC| S1
Client -->|Chunk gRPC| S2

M1 -->|Placement Decisions| S1
M1 -->|Placement Decisions| S2
M1 -->|Placement Decisions| S3
M1 -->|Placement Decisions| S4

S1 -->|Heartbeat gRPC| M1
S2 -->|Heartbeat gRPC| M1
S3 -->|Heartbeat gRPC| M1
S4 -->|Heartbeat gRPC| M1

```

---

# Control Plane vs Data Plane

## Control Plane

- Metadata Raft cluster
- File → chunk mapping
- Chunk → node mapping
- Node health tracking
- Replication decisions
- Placement strategy

## Data Plane

- Chunk storage
- Chunk transfer
- P2P replication
- Client downloads
- Checksum validation

---

# Metadata Cluster Architecture

```mermaid
flowchart LR

Client --> Leader

subgraph RaftCluster
Leader --> F1
Leader --> F2
F1 --> Leader
F2 --> Leader
end

Leader -->|Commit Metadata| StateMachine

```

## Metadata Stored

- file_id → chunk_ids
- chunk_id → replica_nodes
- replication_factor
- node_status
- free_space
- version

## Why Raft

- Leader election
- Strong consistency
- Replicated state machine
- No single point of failure

---

# Storage Node Internal Architecture

```mermaid
flowchart TB

API[gRPC Server]
Store[Chunk Store]
Checksum[Checksum Index]
Replicator[Replication Worker]
HB[Heartbeat Sender]

API --> Store
Store --> Checksum
API --> Replicator
HB --> API

```

## Responsibilities

- Store chunks on disk
- Serve chunks to clients
- Stream chunks to peers
- Verify checksums
- Send heartbeat to metadata

---

# gRPC Interfaces

## Metadata Service

- GetChunkLocations(file)
- AllocateChunk(file, index)
- CommitChunk(chunk_id, node)
- Heartbeat(node_status)
- ReportCorruption(chunk_id)

## Storage Node Service

- PutChunk(stream)
- GetChunk(stream)
- ReplicateChunk(target_node)
- VerifyChunk(chunk_id)

---

# File Upload Flow

```mermaid
sequenceDiagram

participant C as Client
participant M as Metadata Leader
participant A as Storage Node A
participant B as Storage Node B
participant D as Storage Node D

C->>M: Allocate chunk placements
M-->>C: Nodes A,B,D

C->>A: Stream chunk
A->>B: Replicate chunk
A->>D: Replicate chunk

B-->>A: ACK
D-->>A: ACK
A-->>C: Success

A->>M: Commit chunk mapping

```

## Notes

- Client uploads to primary node
- Primary replicates P2P
- Metadata updated only after success
- No write ordering complexity
- Write-once model

---

# File Download Flow

```mermaid
sequenceDiagram

participant C as Client
participant M as Metadata
participant S1 as Storage Node
participant S2 as Storage Node

C->>M: Get chunk locations
M-->>C: Node list per chunk

par Parallel
C->>S1: GetChunk
C->>S2: GetChunk
end

S1-->>C: Chunk data
S2-->>C: Chunk data

Note over C: Verify checksum
Note over C: Reassemble file

```

---

# Replication Repair Flow

```mermaid
sequenceDiagram

participant M as Metadata Leader
participant A as Source Node
participant N as New Node

M->>M: Detect under-replicated chunk
M->>A: Replicate chunk to N
A->>N: Stream chunk
N-->>M: Replication complete

```

---

# Heartbeat & Failure Detection

```mermaid
sequenceDiagram

participant S as Storage Node
participant M as Metadata

loop every 3s
S->>M: Heartbeat(free_space, chunk_count)
end

M->>M: Timeout detection
M->>M: Mark node dead
M->>M: Trigger re-replication

```

---

# Chunk Layout

- Chunk size: configurable (4–8 MB)
- Chunk ID: SHA256(file_id + index)
- Stored as file on disk
- Checksum stored separately

Directory example:

```
chunks/
  ab12.chunk
  9fd3.chunk
checksums.json
```

---

# Placement Strategy

Initial version:

- choose nodes with most free space
- avoid duplicate replica on same node
- round-robin fallback

Advanced option:

- rack/failure-domain labels

---

# Consistency Model

System uses:

**Write Once — Read Many**

- No concurrent writes
- No distributed locking needed
- Simplifies replication
- Safe for academic DFS

---

# Fault Tolerance

## Metadata Layer

- Raft majority commit
- Leader election
- Log replication

## Storage Layer

- Replication factor = 3
- Automatic repair
- Checksum validation

---

# Differences from GFS

- Simplified write pipeline
- No lease-based write ordering
- No rack-awareness (optional)
- Raft instead of custom master journal

---

# Differences from BitTorrent

- Managed storage nodes
- Consensus metadata
- Guaranteed replication factor
- Persistent storage responsibility
- Controlled placement

---

# Demo Scenario

- Start 3 metadata nodes
- Leader election visible
- Start 4 storage nodes
- Upload file
- Show chunk distribution
- Kill one node
- Auto re-replication triggers
- File still downloadable

---

# Tech Stack

- Go
- gRPC
- Hashicorp Raft
- SHA256 checksums
- Local disk chunk storage

---

# Implementation Plans

[Storage Node Implementation](./Storage%20Node%20Implementation.md)

[Client Implementation](./Client%20Implementation.md)