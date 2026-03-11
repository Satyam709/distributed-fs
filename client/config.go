package client

import (
	"os"
	"strconv"
)

type Config struct {
	MetadataAddrs        []string
	ChunkSize            int64
	MaxParallelUploads   int
	MaxParallelDownloads int
	ManifestDir          string
	OutputDir            string
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
	}
}

// LoadFromEnv allows overriding defaults via environment variables.
func LoadFromEnv() *Config {
	cfg := DefaultConfig()
	if val := os.Getenv("CHUNK_SIZE"); val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			cfg.ChunkSize = i
		}
	}
	// Add more environment checks as needed for metadata addrs, etc.
	return cfg
}
