package dfsclientconfig

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/satyam709/distributed-fs/internal/retry"
)

const (
	MinChunkSize = 64 * 1024
	MaxChunkSize = 64 * 1024 * 1024
	MinFrameSize = 4 * 1024
	MaxFrameSize = 4 * 1024 * 1024
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
	RetryBaseBackoff     time.Duration
	RetryMaxBackoff      time.Duration
	RPCTimeout           time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		MetadataAddrs:        []string{"localhost:50050"},
		ChunkSize:            4 * 1024 * 1024,
		MaxParallelUploads:   4,
		MaxParallelDownloads: 4,
		ManifestDir:          "./manifests_logs",
		OutputDir:            "./downloads_logs",
		RetryAttempts:        5,
		FrameSize:            32 * 1024,
		RetryBaseBackoff:     100 * time.Millisecond,
		RetryMaxBackoff:      5 * time.Second,
		RPCTimeout:           10 * time.Second,
	}
}

func (c *Config) RetryPolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts: c.RetryAttempts,
		Base:        c.RetryBaseBackoff,
		Max:         c.RetryMaxBackoff,
		Multiplier:  2.0,
	}
}

func (c *Config) Validate() error {
	if c.ChunkSize < MinChunkSize || c.ChunkSize > MaxChunkSize {
		return fmt.Errorf("dfsclientconfig: chunk_size %d out of range [%d, %d]",
			c.ChunkSize, MinChunkSize, MaxChunkSize)
	}
	if c.FrameSize < MinFrameSize || c.FrameSize > MaxFrameSize {
		return fmt.Errorf("dfsclientconfig: frame_size %d out of range [%d, %d]",
			c.FrameSize, MinFrameSize, MaxFrameSize)
	}
	if c.RetryAttempts < 1 {
		return fmt.Errorf("dfsclientconfig: RetryAttempts must be >= 1, got %d", c.RetryAttempts)
	}
	if c.RetryBaseBackoff <= 0 {
		return fmt.Errorf("dfsclientconfig: RetryBaseBackoff must be > 0, got %v", c.RetryBaseBackoff)
	}
	if c.RetryMaxBackoff < c.RetryBaseBackoff {
		return fmt.Errorf("dfsclientconfig: RetryMaxBackoff (%v) must be >= RetryBaseBackoff (%v)",
			c.RetryMaxBackoff, c.RetryBaseBackoff)
	}
	if c.RPCTimeout <= 0 {
		return fmt.Errorf("dfsclientconfig: RPCTimeout must be > 0, got %v", c.RPCTimeout)
	}
	return nil
}

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
	if val := os.Getenv("RETRY_BASE_BACKOFF"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			cfg.RetryBaseBackoff = d
		}
	}
	if val := os.Getenv("RETRY_MAX_BACKOFF"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			cfg.RetryMaxBackoff = d
		}
	}
	if val := os.Getenv("RPC_TIMEOUT"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			cfg.RPCTimeout = d
		}
	}

	return cfg
}
