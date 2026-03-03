# Client Implementation

### 1. Config

Single struct the whole client reads from. Needs: metadata addresses, chunk size (default 4MB), max parallel uploads, max parallel downloads, manifest directory, output directory. Load from a config file or environment variables.

---

### 2. Chunker

Takes a file path, produces an ordered list of chunk descriptors. Responsibilities:

- Open file, compute total size, calculate chunk count via `ceil(size / chunkSize)`
- Generate `file_id` — UUID, generated once per file before anything else
- For each chunk: compute `chunk_id = SHA256(file_id + chunk_index)`, record byte offset and size
- Does NOT read chunk bytes yet — just produces the descriptor list
- Last chunk will be smaller than chunk size — handle this correctly

Output per chunk descriptor: `chunk_id`, `file_id`, `chunk_index`, `offset`, `size`

---

### 3. Manifest

Written to disk before a single byte is uploaded. Used for resume if upload crashes halfway.

Responsibilities:

- Write manifest file at start of upload: `file_id`, `filename`, `total_size`, `chunk_size`, per-chunk status (pending/done)
- Update individual chunk status to done as each chunk completes
- On upload start, check if manifest already exists for this file — if yes, resume by skipping done chunks
- Delete manifest file after successful `CommitFile`
- Manifest file named by `file_id` so it's unique per upload attempt

---

### 4. Metadata Client

Thin wrapper around the generated MetadataService gRPC stub. Hides raw proto types from the rest of the client. Methods needed:

- `CreateFile(file_id, filename, size, chunk_ids)` → placement map per chunk (primary node + replica nodes)
- `CommitFile(file_id)` → ack
- `GetFile(file_id or filename)` → file record with ordered chunk list and live node addresses per chunk
- `ListFiles()` → list of file metadata
- `GetChunkLocations(chunk_id)` → live node addresses

Handles leader redirect — if metadata follower responds with "not leader, go to X", client retries against the leader address automatically.

---

### 5. Storage Client

Thin wrapper around StorageService gRPC stubs. One instance per storage node address, managed via a small connection pool (reuse connections across chunks going to the same node).

Methods needed:

- `PutChunk(chunk_id, file_id, chunk_index, data, checksum, replicate_to)` → opens client stream, sends frames, closes and receives response
- `GetChunk(chunk_id, offset)` → opens server stream, receives frames, writes to provided writer

Framing logic lives here — PutChunk splits data into 32KB frames and sends them one by one. GetChunk receives frames and assembles into the provided writer.

---

### 6. ParallelUploader

Orchestrates the full upload of one file. Takes chunk descriptors + placement map from metadata.

Responsibilities:

- Semaphore-limited goroutine pool — max N concurrent chunk uploads (configurable, default 4)
- For each chunk: read bytes from file at the correct offset, compute checksum, call `StorageClient.PutChunk`
- On success: update manifest status to done
- On failure: retry up to 3 times before marking failed
- Collect results — report which chunks succeeded and which failed
- Does NOT call CreateFile or CommitFile — that's the orchestrator's job

Key detail: reads from the source file at the chunk's byte offset. Multiple goroutines read from the same file handle concurrently using `ReadAt` (which is safe — no seek needed).

---

### 7. ParallelDownloader

Mirror of ParallelUploader. Takes chunk list with node addresses from metadata.

Responsibilities:

- Semaphore-limited goroutine pool — max N concurrent chunk downloads
- For each chunk: pick a live replica (round robin or least loaded), call `StorageClient.GetChunk`
- Write received bytes into a pre-allocated output buffer at the correct offset
- Verify checksum after receiving each chunk
- On checksum failure: retry against a different replica
- On all replicas failing for a chunk: return error
- After all chunks done: write assembled buffer to output file

Key detail: pre-allocate output file to full size before downloading. Each goroutine writes to its own byte range — no coordination needed, no locks.

---

### 8. CLI (Cobra)

Three commands. Each command wires together the components above in the right order.

**upload command**

```
client upload --file <path> --name <filename>
```

Flow:

1. Config → Chunker → chunk descriptors
2. Manifest.Create
3. MetadataClient.CreateFile → placement map
4. ParallelUploader.Upload
5. MetadataClient.CommitFile
6. Manifest.Delete

**download command**

```
client download --name <filename> --out <path>
```

Flow:

1. MetadataClient.GetFile → chunk list with node addresses
2. ParallelDownloader.Download → assembled file at output path

**list command**

```
client list
```

Flow:

1. MetadataClient.ListFiles → print table of files, sizes, chunk counts

---

### 9. Checksum Helper

Shared utility. Two functions:

- `Compute(data []byte) string` — returns hex SHA256
- `ComputeChunkID(fileID string, index int) string` — returns hex SHA256 of fileID+index string

Used by Chunker (to generate chunk IDs) and ParallelUploader (to checksum chunk data before upload). Lives in `internal/checksum/` so storage node can use the same helper.

---

## Build Order

```
Config + Checksum helper      ← day 1, no dependencies
    └── Chunker               ← only needs config + checksum
          └── Manifest        ← only needs chunk descriptors
                └── MetadataClient    ← only needs proto gen + config
                      └── StorageClient     ← only needs proto gen + config
                            └── ParallelUploader  ← needs all above
                            └── ParallelDownloader ← needs all above
                                  └── CLI          ← wires everything, last
```

---

## Component Responsibility Boundaries

| Component | Knows About | Does NOT Know About |
| --- | --- | --- |
| Chunker | file path, chunk size, SHA256 | network, gRPC, metadata |
| Manifest | file system, chunk status | network, gRPC |
| MetadataClient | metadata gRPC, proto types | storage nodes, chunk bytes |
| StorageClient | storage gRPC, framing | metadata, chunk descriptors |
| ParallelUploader | file reading, semaphore, StorageClient | metadata, manifest |
| ParallelDownloader | output file writing, semaphore, StorageClient | metadata, manifest |
| CLI | everything — wires it all | implementation details |

Clean boundaries mean each component is independently testable. Chunker tests need no network. StorageClient tests need no file system. ParallelUploader tests can mock StorageClient.