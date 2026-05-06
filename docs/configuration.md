# Configuration Guide

## How Components Link Together

```
                                 ┌──────────────────────┐
                                 │   Metadata Cluster    │
                                 │   (Raft consensus)    │
                                 │                       │
                                 │  meta-1 :4001 :5001   │
                                 │  meta-2 :4002 :5002   │
                                 │  meta-3 :4003 :5003   │
                                 └──┬──────┬──────┬──────┘
                                    │      │      │
                          register  │      │      │  heartbeat
                          heartbeats│      │      │  commit_chunk
                                    │      │      │
               ┌────────────────────┼──────┼──────┼──────────────┐
               │                    ▼      ▼      ▼              │
               │           ┌────────────────────────┐             │
               │           │    Storage Nodes        │             │
               │           │                         │             │
         ┌─────┴──────┐    │  storage-1 :4000        │ ── P2P ──  │
         │   Client   │    │  storage-2 :4000        │ Replicate  │
         │             │    │  storage-3 :4000        │   Chunk    │
         │  dfs-cli    │───►│                         │             │
         │             │    └────────────────────────┘             │
         └────────────┘                                           │
```

The system uses three components connected via gRPC:

1. **Metadata Cluster** — storage nodes register, heartbeat, and commit chunks. Clients create/list/get files and get chunk placements.
2. **Storage Nodes** — clients upload and download chunks. Storage nodes replicate chunks to each other (P2P).
3. **Client** — talks to metadata for file operations and to storage nodes for chunk operations.

---

## Configuration Methods

All components support two configuration methods (priority order):

1. **JSON config file** — set via `*_CONFIG` env var pointing to a file path
2. **Environment variables** — direct `*_PRESET` vars override specific fields

Environment variables take precedence over JSON file values. Defaults are applied first.

---

## Metadata Node Configuration

### Key Settings for Linking

| Setting | Purpose |
|---|---|
| `METADATA_PEER_ADDRS` | Lists other metadata nodes to form Raft cluster |
| `METADATA_BOOTSTRAP` | `true` for the first node in a new cluster |
| `METADATA_RAFT_ADVERTISE` | Advertised Raft address (required for Docker/multi-host) |

### Typical Docker Deployment

```yaml
environment:
  - METADATA_NODE_ID=metadata-1
  - METADATA_GRPC_ADDR=:4001
  - METADATA_RAFT_ADDR=0.0.0.0:5001
  - METADATA_RAFT_ADVERTISE=metadata-1:5001
  - METADATA_RAFT_DIR=/metadata-node/data/raft
  - METADATA_BOOTSTRAP=true
  - METADATA_PEER_ADDRS=metadata-2:metadata-2:5002,metadata-3:metadata-3:5003
  - METADATA_REPLICATION_FACTOR=3
```

Full config reference: [docs/metadata.md](metadata.md)

---

## Storage Node Configuration

### Key Settings for Linking

| Setting | Purpose |
|---|---|
| `STORAGE_METADATA_ADDRS` | Points to one or more metadata nodes |
| `STORAGE_ADVERTISE_ADDR` | Address metadata uses to reach this node (required for Docker/multi-host) |

### Typical Docker Deployment

```yaml
environment:
  - STORAGE_NODE_ID=storage-1
  - STORAGE_GRPC_ADDR=:4000
  - STORAGE_ADVERTISE_ADDR=storage-1:4000
  - STORAGE_METADATA_ADDRS=metadata-1:4001
  - STORAGE_DATA_DIR=/storage_node/data
  - STORAGE_REPLICATION_FACTOR=3
```

Storage nodes connect to metadata on startup via `RegisterNode`. After registration, they send heartbeats every 3s. Metadata never connects to storage nodes directly — all communication is initiated by storage nodes.

Full config reference: [docs/storage.md](storage.md)

---

## Client Configuration

### Key Setting for Linking

| Setting | Purpose |
|---|---|
| `METADATA_ADDRS` | Comma-separated list of metadata node gRPC addresses |

### Typical Usage

```bash
export METADATA_ADDRS="metadata-1:4001,metadata-2:4001,metadata-3:4001"
./bin/client/dfs-cli upload --file ./data.csv --name data.csv
```

The client discovers the Raft leader automatically and retries with redirect. It also connects directly to storage nodes based on chunk placements returned by metadata.

Full config reference: [docs/client.md](client.md)

---

## Port Reference

| Component | Port | Protocol | Description |
|---|---|---|---|
| Metadata | 4001 (default) | gRPC | Client and storage node API |
| Metadata | 5001 (default) | TCP | Raft internal communication |
| Storage | 4000 (default) | gRPC | Client chunk operations + P2P replication |

In multi-node Docker deployments, the `run-cluster.sh` script auto-assigns ports:

| Node | gRPC Host Port | Raft Host Port |
|---|---|---|
| metadata-1 | 4001 | 5001 |
| metadata-2 | 4002 | 5002 |
| metadata-3 | 4003 | 5003 |
| storage-1 | 4101 → 4000 | — |
| storage-2 | 4102 → 4000 | — |
| storage-3 | 4103 → 4000 | — |
| storage-N | 4100+N → 4000 | — |

---

## Running the Cluster

The `scripts/run-cluster.sh` script dynamically generates a `docker-compose.yml` with all required environment variables pre-configured:

```bash
# 1 metadata + 3 storage
./scripts/run-cluster.sh -m 1 -s 3 up -d --build

# 3 metadata + 5 storage + client container
./scripts/run-cluster.sh -m 3 -s 5 -c up -d --build

# View generated compose file
cat scripts/docker-compose.yml

# Stop cluster
./scripts/run-cluster.sh down
```
