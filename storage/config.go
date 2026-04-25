package storage

import (
	"errors"
	"time"
)

type StorageNodeConfig struct {
	NodeID string `json:"node_id"`

	GRPCAddr     string `json:"grpc_addr"`
	MetadataAddr string `json:"metadata_addr"`

	Timeout           time.Duration `json:"timeout"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	DataDir string `json:"data_dir"`

	ReplicationFactor int `json:"replication_factor"`
}

func DefaultStorageNodeConfig() *StorageNodeConfig {
	return &StorageNodeConfig{
		GRPCAddr:          ":4000",
		Timeout:           120 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		DataDir:           "./data",
		ReplicationFactor: 3,
	}
}

func (c *StorageNodeConfig) Validate() error {
	if c.GRPCAddr == "" {
		return errors.New("StorageNodeConfig: GRPCAddr must not be empty")
	}
	if c.MetadataAddr == "" {
		return errors.New("StorageNodeConfig: MetadataAddr must not be empty")
	}
	if c.DataDir == "" {
		return errors.New("StorageNodeConfig: DataDir must not be empty")
	}
	if c.ReplicationFactor < 1 {
		return errors.New("StorageNodeConfig: ReplicationFactor must be at least 1")
	}
	return nil
}
