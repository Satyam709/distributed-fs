package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	DefaultStorageGRPCAddr          = ":4000"
	DefaultStorageMetadataAddr      = ":3000"
	DefaultStorageDataDir           = "./data"
	DefaultStorageTimeout           = 120 * time.Second
	DefaultStorageHeartbeatInterval = 3 * time.Second
	DefaultStorageReplicationFactor = 3
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
	cfg := &StorageNodeConfig{
		GRPCAddr:          DefaultStorageGRPCAddr,
		MetadataAddr:      DefaultStorageMetadataAddr,
		Timeout:           DefaultStorageTimeout,
		HeartbeatInterval: DefaultStorageHeartbeatInterval,
		DataDir:           DefaultStorageDataDir,
		ReplicationFactor: DefaultStorageReplicationFactor,
	}
	cfg.Default()
	return cfg
}

func LoadFromJSON(path string) (*StorageNodeConfig, error) {
	cfg := DefaultStorageNodeConfig()
	if err := cfg.ApplyJSON(path); err != nil {
		return nil, err
	}
	cfg.Default()
	return cfg, nil
}

func (c *StorageNodeConfig) ApplyJSON(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var raw struct {
		NodeID            *string         `json:"node_id"`
		GRPCAddr          *string         `json:"grpc_addr"`
		MetadataAddr      *string         `json:"metadata_addr"`
		Timeout           json.RawMessage `json:"timeout"`
		HeartbeatInterval json.RawMessage `json:"heartbeat_interval"`
		DataDir           *string         `json:"data_dir"`
		ReplicationFactor *int            `json:"replication_factor"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.NodeID != nil {
		c.NodeID = *raw.NodeID
	}
	if raw.GRPCAddr != nil {
		c.GRPCAddr = *raw.GRPCAddr
	}
	if raw.MetadataAddr != nil {
		c.MetadataAddr = *raw.MetadataAddr
	}
	if raw.DataDir != nil {
		c.DataDir = *raw.DataDir
	}
	if raw.ReplicationFactor != nil {
		c.ReplicationFactor = *raw.ReplicationFactor
	}

	if err := applyDurationJSON(raw.Timeout, &c.Timeout); err != nil {
		return fmt.Errorf("config: timeout: %w", err)
	}
	if err := applyDurationJSON(raw.HeartbeatInterval, &c.HeartbeatInterval); err != nil {
		return fmt.Errorf("config: heartbeat_interval: %w", err)
	}

	return nil
}

func (c *StorageNodeConfig) ApplyEnv() {
	if v := os.Getenv("STORAGE_NODE_ID"); v != "" {
		c.NodeID = v
	}
	if v := os.Getenv("STORAGE_GRPC_ADDR"); v != "" {
		c.GRPCAddr = v
	}
	if v := os.Getenv("STORAGE_METADATA_ADDR"); v != "" {
		c.MetadataAddr = v
	}
	if v := os.Getenv("STORAGE_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv("STORAGE_REPLICATION_FACTOR"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			c.ReplicationFactor = parsed
		}
	}
	applyDurationEnv("STORAGE_TIMEOUT", &c.Timeout)
	applyDurationEnv("STORAGE_HEARTBEAT_INTERVAL", &c.HeartbeatInterval)
}

func applyDurationJSON(raw json.RawMessage, target *time.Duration) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, err := time.ParseDuration(asString)
		if err != nil {
			return err
		}
		*target = d
		return nil
	}

	var asNumber float64
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		*target = time.Duration(asNumber) * time.Second
		return nil
	}

	return errors.New("must be duration string or number of seconds")
}

func applyDurationEnv(key string, target *time.Duration) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	if d, err := time.ParseDuration(v); err == nil {
		*target = d
		return
	}
	if sec, err := strconv.Atoi(v); err == nil {
		*target = time.Duration(sec) * time.Second
	}
}

func (c *StorageNodeConfig) Default() {
	if c.GRPCAddr == "" {
		c.GRPCAddr = DefaultStorageGRPCAddr
	}
	if c.MetadataAddr == "" {
		c.MetadataAddr = DefaultStorageMetadataAddr
	}
	if c.Timeout <= 0 {
		c.Timeout = DefaultStorageTimeout
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = DefaultStorageHeartbeatInterval
	}
	if c.DataDir == "" {
		c.DataDir = DefaultStorageDataDir
	}
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = DefaultStorageReplicationFactor
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
