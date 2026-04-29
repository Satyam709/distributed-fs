package dfsclientconfig

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Size guardrails — enforced by Validate().
const (
	MinChunkSize = 64 * 1024        // 64 KB
	MaxChunkSize = 64 * 1024 * 1024 // 64 MB
	MinFrameSize = 4 * 1024         // 4 KB
	MaxFrameSize = 4 * 1024 * 1024  // 4 MB (gRPC default max message size)
)

type Config struct {
	MetadataAddrs        []string
	ChunkSize            int64
	MaxParallelUploads   int
	MaxParallelDownloads int
	ManifestDir          string
	OutputDir            string
	RetryAttempts        int
	FrameSize            int
}

// DefaultConfig provides the baseline settings as per implementation specs.
func DefaultConfig() *Config {
	return &Config{
		MetadataAddrs:        []string{"localhost:50050"}, // Metadata leader address
		ChunkSize:            4 * 1024 * 1024,             // Default 4MB
		MaxParallelUploads:   4,                           // Default 4
		MaxParallelDownloads: 4,                           // Default 4
		ManifestDir:          "./manifests_logs",
		OutputDir:            "./downloads_logs",
		RetryAttempts:        3,         // Default 3 retries per chunk
		FrameSize:            32 * 1024, // Default 32KB frames
	}
}

// Validate checks that ChunkSize and FrameSize are within acceptable
// ranges. Called automatically by the SDK constructor.
func (c *Config) Validate() error {
	if c.ChunkSize < MinChunkSize || c.ChunkSize > MaxChunkSize {
		return fmt.Errorf("dfsclientconfig: chunk_size %d out of range [%d, %d]",
			c.ChunkSize, MinChunkSize, MaxChunkSize)
	}
	if c.FrameSize < MinFrameSize || c.FrameSize > MaxFrameSize {
		return fmt.Errorf("dfsclientconfig: frame_size %d out of range [%d, %d]",
			c.FrameSize, MinFrameSize, MaxFrameSize)
	}
	return nil
}

// LoadFromEnv allows overriding defaults via environment variables.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	if val := os.Getenv("METADATA_ADDRS"); val != "" {
		cfg.MetadataAddrs = strings.Split(val, ",")
	}
	if val := os.Getenv("CHUNK_SIZE"); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			cfg.ChunkSize = i
		}
	}
	if val := os.Getenv("MAX_PARALLEL_UPLOADS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			cfg.MaxParallelUploads = i
		}
	}
	if val := os.Getenv("MAX_PARALLEL_DOWNLOADS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			cfg.MaxParallelDownloads = i
		}
	}
	if val := os.Getenv("MANIFEST_DIR"); val != "" {
		cfg.ManifestDir = val
	}
	if val := os.Getenv("OUTPUT_DIR"); val != "" {
		cfg.OutputDir = val
	}
	if val := os.Getenv("RETRY_ATTEMPTS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			cfg.RetryAttempts = i
		}
	}
	if val := os.Getenv("FRAME_SIZE"); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			cfg.FrameSize = i
		}
	}

	return cfg
}
