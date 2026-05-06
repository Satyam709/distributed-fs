# DFS CLI Client

## Overview

The DFS client (`dfs-cli`) is a command-line tool for interacting with the distributed file system. It handles file chunking, parallel upload/download with gRPC streaming, resumable uploads via local manifests, and transparent leader redirect for metadata operations. Built in Go using Cobra for CLI and gRPC for RPC.

---

## Configuration

### Config Loading

Configuration is loaded at startup from environment variables. There is no config file support for the client.

### Environment Variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `METADATA_ADDRS` | string | `localhost:50050` | Comma-separated metadata node addresses |
| `CHUNK_SIZE` | int | `4194304` (4 MB) | Chunk size in bytes (64 KB – 64 MB) |
| `MAX_PARALLEL_UPLOADS` | int | `4` | Concurrent chunk upload streams |
| `MAX_PARALLEL_DOWNLOADS` | int | `4` | Concurrent chunk download streams |
| `MANIFEST_DIR` | string | `./manifests_logs` | Directory for resumable-upload manifests |
| `OUTPUT_DIR` | string | `./downloads_logs` | Default download output directory |
| `RETRY_ATTEMPTS` | int | `5` | Max retry attempts per chunk |
| `FRAME_SIZE` | int | `32768` (32 KB) | gRPC streaming frame size (4 KB – 4 MB) |
| `RETRY_BASE_BACKOFF` | duration | `100ms` | Initial retry backoff |
| `RETRY_MAX_BACKOFF` | duration | `5s` | Maximum retry backoff |
| `RPC_TIMEOUT` | duration | `10s` | Per-RPC call timeout |

### Connecting to a Cluster

Set the metadata addresses to point the client at your cluster:

```bash
export METADATA_ADDRS="10.0.0.1:4001,10.0.0.2:4001,10.0.0.3:4001"
```

The client auto-discovers the Raft leader and transparently retries against the leader on redirect.

---

## CLI Commands

### `upload`

Upload a local file into the DFS.

```
dfs-cli upload --file <path> [--name <filename>]

Flags:
  -f, --file   Local file path to upload (required)
  -n, --name   Logical filename in the DFS (defaults to base name of --file)
```

**Examples:**

```bash
dfs-cli upload --file ./data.csv
dfs-cli upload --file ./data.csv --name reports/january.csv
```

### `download`

Download a file from the DFS.

```
dfs-cli download --name <filename> [--out <path>]

Flags:
  -n, --name   DFS filename to download (required)
  -o, --out    Local output path (defaults to <OUTPUT_DIR>/<basename>)
```

**Example:**

```bash
dfs-cli download --name data.csv --out /tmp/retrieved.csv
```

### `list`

List files stored in the DFS.

```
dfs-cli list [--prefix <prefix>]

Flags:
  -p, --prefix   Filename prefix filter (optional)
```

**Examples:**

```bash
dfs-cli list
dfs-cli list --prefix reports/
```

Output is a table of file metadata (name, size, chunk count, creation time).

### `delete`

Delete a file by its file ID.

```
dfs-cli delete --id <file_id>

Flags:
  --id   UUID of the file to delete (required)
```

**Example:**

```bash
dfs-cli delete --id 550e8400-e29b-41d4-a716-446655440000
```

---

## Upload Flow

```
1. Chunking
   Chunker splits file into fixed-size chunks (chunk_size).
   Chunk IDs = SHA256(file_id + chunk_index) — deterministic, no server needed.
   file_id = UUID, generated once per file.

2. Manifest
   Local manifest created in <manifest_dir>/<file_id>.json.
   Tracks per-chunk status: pending → uploading → done.
   Enables resume if upload is interrupted.

3. Metadata
   MetadataService.CreateFile(file_name, file_id, chunk_ids, total_size)
   Returns chunk placement: primary + replicas per chunk.

4. Parallel Upload
   ParallelUploader dispatches max_parallel_uploads concurrent uploads.
   Each chunk streams to its primary storage node via StorageService.PutChunk.
   Primary handles P2P replication to replica nodes.

5. Commit
   After all chunks uploaded: MetadataService.CommitFile(file_id)

6. Cleanup
   Manifest deleted on success. Persists on failure for resume.
```

### Resumable Uploads

If the CLI is interrupted mid-upload, re-running the same upload command detects the manifest and skips already-completed chunks (`status: done`). Chunks marked `pending` or `uploading` are retried.

---

## Download Flow

```
1. Metadata
   MetadataService.GetFile(file_name)
   Returns ordered chunk list with live node addresses.

2. Parallel Download
   ParallelDownloader dispatches max_parallel_downloads concurrent downloads.
   Each chunk fetched from any live replica via StorageService.GetChunk (server streaming).
   Data streamed in frame_size (32 KB) frames.

3. Checksum Verification
   SHA256 of received data compared against expected chunk_id.
   On mismatch: retry from different replica.

4. Reassembly
   Chunks written to output file in chunk_index order.
```

---

## Error Handling

- **Per-chunk retry**: Failed uploads/downloads retried up to `retry_attempts` with exponential backoff
- **Replica failover**: If one replica fails during download, next available replica is tried
- **Leader redirect**: If metadata call hits a follower, client updates connection and retries against leader

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    dfs-cli (Cobra)                    │
│  upload │ download │ list │ delete                   │
└────────────────────┬─────────────────────────────────┘
                     │
┌────────────────────▼─────────────────────────────────┐
│                   DFSClient                          │
│  NewClient(opts...) → Upload/Download/List/Delete    │
└────┬───────────────────────────────┬─────────────────┘
     │                               │
┌────▼──────────┐              ┌─────▼─────────────────┐
│ MetadataClient│              │   StorageClient        │
│ CreateFile    │              │   PutChunk (stream)    │
│ GetFile       │              │   GetChunk (stream)    │
│ ListFiles     │              │                        │
│ DeleteFile    │              │                        │
│ CommitFile    │              │                        │
└────┬──────────┘              └─────┬──────────────────┘
     │                               │
┌────▼──────────┐              ┌─────▼──────────────────┐
│   Chunker     │              │  ParallelUploader       │
│ Split file    │              │  ParallelDownloader    │
│ Chunk ID gen  │              └─────┬──────────────────┘
└───────────────┘                    │
                          ┌──────────▼─────────────────┐
                          │        Manifest             │
                          │  Read/Write chunk status    │
                          │  Resume logic               │
                          └────────────────────────────┘
```

---

## Package Structure

| Path | Purpose |
|---|---|
| `client/cmd/dfs-cli/main.go` | Cobra CLI entry point |
| `client/dfsclient/client.go` | `DFSClient` — top-level API |
| `client/dfsclient/options.go` | Functional options for client construction |
| `client/dfsclient/types.go` | `ProgressInfo`, `UploadResult`, `DownloadResult`, `FileInfo` |
| `client/internal/chunker/chunker.go` | File splitting and chunk ID generation |
| `client/internal/dfsclientconfig/config.go` | Configuration struct and loading |
| `client/internal/manifest/manifest.go` | Local manifest for resumable uploads |
| `client/internal/metadataclient/client.go` | `MetadataClient` interface |
| `client/internal/metadataclient/grpc.go` | gRPC `MetadataClient` implementation |
| `client/internal/storageclient/client.go` | `StorageClient` interface |
| `client/internal/storageclient/grpc.go` | gRPC `StorageClient` implementation |
| `client/internal/service/upload.go` | `ParallelUploader` |
| `client/internal/service/download.go` | `ParallelDownloader` |
| `client/internal/service/list.go` | `ListFiles` logic |
