package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/satyam709/distributed-fs/internal/logging"
)

// Defaults
const (
	DefaultStorageGRPCAddr          = ":4000"
	DefaultStorageMetadataAddr      = ":3000"
	DefaultStorageDataDir           = "./data"
	DefaultStorageTimeout           = 120 * time.Second
	DefaultStorageHeartbeatInterval = 3 * time.Second
	DefaultStorageReplicationFactor = 3
)

// EnvironmentVariables
const (
	EnvStorageNodeID            string = "STORAGE_NODE_ID"
	EnvStorageGrpcAddr          string = "STORAGE_GRPC_ADDR"
	EnvStorageMetadataAddr      string = "STORAGE_METADATA_ADDR"
	EnvStorageDataDir           string = "STORAGE_DATA_DIR"
	EnvStorageReplicationFactor string = "STORAGE_REPLICATION_FACTOR"
	EnvStorageTimeout           string = "STORAGE_TIMEOUT"
	EnvStorageHeartbeatInterval string = "STORAGE_HEARTBEAT_INTERVAL"
	EnvStorageJsonConfigPath    string = "STORAGE_CONFIG"
)

type StorageNodeConfig struct {
	NodeID string `json:"node_id"`

	GRPCAddr     string `json:"grpc_addr"`
	MetadataAddr string `json:"metadata_addr"`

	Timeout           time.Duration `json:"timeout"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	DataDir string `json:"data_dir"`

	ReplicationFactor int `json:"replication_factor"`

	logger *logging.CLogger
}

// DefaultStorageNodeConfig returns the config with Defaults
func DefaultStorageNodeConfig() *StorageNodeConfig {
	cfg := &StorageNodeConfig{
		logger: logging.NewCLogger().With("component", "Config"),
	}
	cfg.Default()
	return cfg
}

// LoadConfigDefaultFlow is first loads a defaults config and than looks for json config to apply overrides
// next it looks for env var to override further
// LoadFlow: Defaults, Json, Env => increasing order of priority
func LoadConfigDefaultFlow() (*StorageNodeConfig, error) {
	cfg := DefaultStorageNodeConfig()

	if configPath := os.Getenv(EnvStorageJsonConfigPath); configPath != "" {
		if err := cfg.ApplyJSON(configPath); err != nil {
			return nil, fmt.Errorf("failed to load config from %s: %w", configPath, err)
		}
	}

	cfg.ApplyEnv()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to load config %w", err)
	}

	return cfg, nil
}

// LoadFromJSON init a default config and loads the config overrides from the json path
// return nil config in case of error
func LoadFromJSON(path string) (*StorageNodeConfig, error) {
	cfg := DefaultStorageNodeConfig()
	if err := cfg.ApplyJSON(path); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ApplyJSON it applies the values from a json config and return a nil config
// with error if json has invalid entries
func (c *StorageNodeConfig) ApplyJSON(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var raw struct {
		NodeID            *string `json:"node_id"`
		GRPCAddr          *string `json:"grpc_addr"`
		MetadataAddr      *string `json:"metadata_addr"`
		Timeout           *string `json:"timeout"`
		HeartbeatInterval *string `json:"heartbeat_interval"`
		DataDir           *string `json:"data_dir"`
		ReplicationFactor *int    `json:"replication_factor"`
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

	if raw.Timeout != nil {
		if c.Timeout, err = time.ParseDuration(*raw.Timeout); err != nil {
			return fmt.Errorf("config: timeout: %w", err)
		}
	}

	if raw.HeartbeatInterval != nil {
		if c.HeartbeatInterval, err = time.ParseDuration(*raw.HeartbeatInterval); err != nil {
			return fmt.Errorf("config: heartbeat_interval: %w", err)
		}
	}

	return nil
}

// ApplyEnv tries to override the existing conig with the values from EnvironmentVariables
// The valid values are applied rest are left unmodified
func (c *StorageNodeConfig) ApplyEnv() {
	if v := os.Getenv(EnvStorageNodeID); v != "" {
		c.NodeID = v
	}
	if v := os.Getenv(EnvStorageGrpcAddr); v != "" {
		c.GRPCAddr = v
	}
	if v := os.Getenv(EnvStorageMetadataAddr); v != "" {
		c.MetadataAddr = v
	}
	if v := os.Getenv(EnvStorageDataDir); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv(EnvStorageReplicationFactor); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			c.ReplicationFactor = parsed
		} else {
			c.logger.Warn("invalid env var using default",
				slog.String("env", EnvStorageReplicationFactor),
				slog.Int("default", DefaultStorageReplicationFactor))
		}
	}
	c.applyDurationEnv(EnvStorageTimeout, &c.Timeout)
	c.applyDurationEnv(EnvStorageHeartbeatInterval, &c.HeartbeatInterval)
}

func (c *StorageNodeConfig) applyDurationEnv(key string, target *time.Duration) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	if d, err := time.ParseDuration(v); err == nil {
		*target = d
		return
	} else {
		c.logger.Warn("invalid env var using default",
			slog.String("env", key))
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


func LoadOrGenerateNodeID(dataDir string, logger *logging.CLogger) string {
	nodeIDPath := filepath.Join(dataDir, "node_id")

	data, err := os.ReadFile(nodeIDPath)
	if err == nil && len(data) > 0 {
		nodeID := string(data)
		logger.Info("loaded existing node_id", slog.String("nodeID", nodeID))
		return nodeID
	}

	nodeID := uuid.New().String()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.FatalError("failed to create data directory", err)
	}
	if err := os.WriteFile(nodeIDPath, []byte(nodeID), 0644); err != nil {
		logger.FatalError("failed to write node_id file", err)
	}
	logger.Info("generated new node_id", slog.String("nodeID", nodeID))
	return nodeID
}