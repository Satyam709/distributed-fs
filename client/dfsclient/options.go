package dfsclient

import "github.com/satyam709/distributed-fs/client/internal/dfsclientconfig"

// Option configures a Client. Use the With* functions to create Options.
type Option func(*dfsclientconfig.Config)

// WithMetadataAddrs sets the metadata service addresses to connect to.
func WithMetadataAddrs(addrs ...string) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.MetadataAddrs = addrs
	}
}

// WithChunkSize sets the chunk size in bytes for splitting files.
// Default is 4MB.
func WithChunkSize(size int64) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.ChunkSize = size
	}
}

// WithMaxParallelUploads sets the maximum number of concurrent chunk uploads.
// Default is 4.
func WithMaxParallelUploads(n int) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.MaxParallelUploads = n
	}
}

// WithMaxParallelDownloads sets the maximum number of concurrent chunk downloads.
// Default is 4.
func WithMaxParallelDownloads(n int) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.MaxParallelDownloads = n
	}
}

// WithManifestDir sets the directory for upload crash-recovery manifests.
func WithManifestDir(dir string) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.ManifestDir = dir
	}
}

// WithOutputDir sets the default output directory for downloads.
func WithOutputDir(dir string) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.OutputDir = dir
	}
}

// WithRetryAttempts sets the number of retry attempts per chunk on failure.
// Default is 3.
func WithRetryAttempts(n int) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.RetryAttempts = n
	}
}

// WithFrameSize sets the gRPC streaming frame size in bytes.
// Default is 32KB.
func WithFrameSize(size int) Option {
	return func(cfg *dfsclientconfig.Config) {
		cfg.FrameSize = size
	}
}

// WithConfig uses an existing Config as the base, overriding defaults.
// Useful with client.LoadFromEnv().
func WithConfig(base *dfsclientconfig.Config) Option {
	return func(cfg *dfsclientconfig.Config) {
		*cfg = *base
	}
}
