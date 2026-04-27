// Package dfsclient provides a clean SDK for interacting with the
// distributed file system. It is the only public API — all internal
// packages (chunker, manifest, metadataclient, storageclient, service)
// are hidden behind this façade.
//
// Usage:
//
//	client, err := dfsclient.New(dfsclient.WithMetadataAddrs("localhost:50050"))
//	if err != nil { ... }
//	defer client.Close()
//
//	result, err := client.Upload(ctx, "/path/to/file", "remote-name.bin", nil)
//	files, err := client.List(ctx, "")
//	result, err := client.Download(ctx, "remote-name.bin", "./output", nil)
//	err = client.Delete(ctx, fileID)
package dfsclient

// ProgressInfo carries detailed progress state for a single chunk operation.
// Designed for building rich UIs (CLI progress bars, web dashboards, etc.).
type ProgressInfo struct {
	// ChunkIndex is the 0-based index of the chunk being processed.
	ChunkIndex int
	// ChunksTotal is the total number of chunks in the operation.
	ChunksTotal int
	// BytesDone is the cumulative bytes transferred so far (across all chunks).
	BytesDone int64
	// BytesTotal is the total file size in bytes.
	BytesTotal int64
	// Err is non-nil if this chunk failed.
	Err error
}

// Percent returns the completion percentage (0–100).
func (p ProgressInfo) Percent() float64 {
	if p.BytesTotal == 0 {
		return 0
	}
	return float64(p.BytesDone) / float64(p.BytesTotal) * 100
}

// ProgressFunc is called after each chunk completes (or fails) during
// upload or download. Implementations should be safe for concurrent use.
type ProgressFunc func(info ProgressInfo)

// UploadResult contains the outcome of an upload operation.
type UploadResult struct {
	// FileID is the identifier assigned to the uploaded file.
	FileID string
	// FileName is the remote name the file was stored under.
	FileName string
	// TotalSize is the file size in bytes.
	TotalSize int64
	// ChunksTotal is the number of chunks the file was split into.
	ChunksTotal int
	// ChunksDone is the number of chunks successfully uploaded.
	ChunksDone int
	// ChunksFailed is the number of chunks that failed to upload.
	ChunksFailed int
}

// DownloadResult contains the outcome of a download operation.
type DownloadResult struct {
	// FileID is the identifier of the downloaded file.
	FileID string
	// FileName is the remote name of the file.
	FileName string
	// TotalSize is the file size in bytes.
	TotalSize int64
	// OutputPath is the local path where the file was written.
	OutputPath string
}

// FileInfo represents a file stored in the distributed FS.
// This is an SDK-owned type decoupled from internal metadata types.
type FileInfo struct {
	// FileID is the unique identifier for the file.
	FileID string
	// FileName is the human-readable name of the file.
	FileName string
	// FileSize is the total size in bytes.
	FileSize int64
	// ChunkSize is the size of each chunk in bytes.
	ChunkSize int64
	// ChunkCount is the number of chunks the file is split into.
	ChunkCount int
	// Status is the current status of the file (e.g. "complete", "creating").
	Status string
	// CreatedAt is the Unix timestamp when the file was created.
	CreatedAt int64
}
