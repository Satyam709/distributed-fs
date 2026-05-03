package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/satyam709/distributed-fs/internal/logging"
)

const (
	DefaultStorageGRPCAddr          = ":4000"
	DefaultStorageMetadataAddr      = ":3000"
	DefaultStorageDataDir           = "./data"
	DefaultStorageTimeout           = 120 * time.Second
	DefaultStorageHeartbeatInterval = 3 * time.Second
	DefaultStorageReplicationFactor = 3
	DefaultStorageRPCTimeout        = 10 * time.Second
	DefaultStorageRetryMaxAttempts  = 5
	DefaultStorageRetryBaseBackoff  = 100 * time.Millisecond
	DefaultStorageRetryMaxBackoff   = 5 * time.Second
)

const (
	EnvStorageNodeID            string = "STORAGE_NODE_ID"
	EnvStorageGrpcAddr          string = "STORAGE_GRPC_ADDR"
	EnvStorageMetadataAddrs     string = "STORAGE_METADATA_ADDRS"
	EnvStorageDataDir           string = "STORAGE_DATA_DIR"
	EnvStorageReplicationFactor string = "STORAGE_REPLICATION_FACTOR"
	EnvStorageTimeout           string = "STORAGE_TIMEOUT"
	EnvStorageHeartbeatInterval string = "STORAGE_HEARTBEAT_INTERVAL"
	EnvStorageRPCTimeout        string = "STORAGE_RPC_TIMEOUT"
	EnvStorageRetryMaxAttempts  string = "STORAGE_RETRY_MAX_ATTEMPTS"
	EnvStorageRetryBaseBackoff  string = "STORAGE_RETRY_BASE_BACKOFF"
	EnvStorageRetryMaxBackoff   string = "STORAGE_RETRY_MAX_BACKOFF"
	EnvStorageJsonConfigPath    string = "STORAGE_CONFIG"
)

type StorageNodeConfig struct {
	NodeID string `json:"node_id"`

	GRPCAddr      string   `json:"grpc_addr"`
	MetadataAddrs []string `json:"metadata_addrs"`

	Timeout           time.Duration `json:"timeout"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	DataDir string `json:"data_dir"`

	ReplicationFactor int `json:"replication_factor"`

	RPCTimeout       time.Duration `json:"rpc_timeout"`
	RetryMaxAttempts int           `json:"retry_max_attempts"`
	RetryBaseBackoff time.Duration `json:"retry_base_backoff"`
	RetryMaxBackoff  time.Duration `json:"retry_max_backoff"`
}

func DefaultStorageNodeConfig() *StorageNodeConfig {
	cfg := &StorageNodeConfig{}
	cfg.Default()
	return cfg
}

func LoadConfigDefaultFlow() (*StorageNodeConfig, error) {
	cfg := DefaultStorageNodeConfig()

	if configPath := os.Getenv(EnvStorageJsonConfigPath); configPath != "" {
		if err := cfg.ApplyJSON(configPath); err != nil {
			return nil, fmt.Errorf("failed to load config from %s: %w", configPath, err)
		}
	}

	if err := cfg.ApplyEnv(); err != nil {
		return nil, fmt.Errorf("failed to load config %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("failed to load config %w", err)
	}

	return cfg, nil
}

func LoadFromJSON(path string) (*StorageNodeConfig, error) {
	cfg := DefaultStorageNodeConfig()
	if err := cfg.ApplyJSON(path); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *StorageNodeConfig) ApplyJSON(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var raw struct {
		NodeID            *string   `json:"node_id"`
		GRPCAddr          *string   `json:"grpc_addr"`
		MetadataAddrs     *[]string `json:"metadata_addrs"`
		Timeout           *string   `json:"timeout"`
		HeartbeatInterval *string   `json:"heartbeat_interval"`
		DataDir           *string   `json:"data_dir"`
		ReplicationFactor *int      `json:"replication_factor"`
		RPCTimeout        *string   `json:"rpc_timeout"`
		RetryMaxAttempts  *int      `json:"retry_max_attempts"`
		RetryBaseBackoff  *string   `json:"retry_base_backoff"`
		RetryMaxBackoff   *string   `json:"retry_max_backoff"`
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
	if raw.MetadataAddrs != nil {
		c.MetadataAddrs = *raw.MetadataAddrs
	}
	if raw.DataDir != nil {
		c.DataDir = *raw.DataDir
	}
	if raw.ReplicationFactor != nil {
		c.ReplicationFactor = *raw.ReplicationFactor
	}
	if raw.RetryMaxAttempts != nil {
		c.RetryMaxAttempts = *raw.RetryMaxAttempts
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
	if raw.RPCTimeout != nil {
		if c.RPCTimeout, err = time.ParseDuration(*raw.RPCTimeout); err != nil {
			return fmt.Errorf("config: rpc_timeout: %w", err)
		}
	}
	if raw.RetryBaseBackoff != nil {
		if c.RetryBaseBackoff, err = time.ParseDuration(*raw.RetryBaseBackoff); err != nil {
			return fmt.Errorf("config: retry_base_backoff: %w", err)
		}
	}
	if raw.RetryMaxBackoff != nil {
		if c.RetryMaxBackoff, err = time.ParseDuration(*raw.RetryMaxBackoff); err != nil {
			return fmt.Errorf("config: retry_max_backoff: %w", err)
		}
	}

	return nil
}

func (c *StorageNodeConfig) ApplyEnv() error {
	if v := os.Getenv(EnvStorageNodeID); v != "" {
		c.NodeID = v
	}
	if v := os.Getenv(EnvStorageGrpcAddr); v != "" {
		c.GRPCAddr = v
	}
	if v := os.Getenv(EnvStorageMetadataAddrs); v != "" {
		c.MetadataAddrs = strings.Split(v, ",")
	}
	if v := os.Getenv(EnvStorageDataDir); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv(EnvStorageReplicationFactor); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			c.ReplicationFactor = parsed
		} else {
			return fmt.Errorf("invalid env var %s: %w", EnvStorageReplicationFactor, err)
		}
	}
	if v := os.Getenv(EnvStorageRetryMaxAttempts); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			c.RetryMaxAttempts = parsed
		} else {
			return fmt.Errorf("invalid env var %s: %w", EnvStorageRetryMaxAttempts, err)
		}
	}
	if err := c.applyDurationEnv(EnvStorageTimeout, &c.Timeout); err != nil {
		return err
	}
	if err := c.applyDurationEnv(EnvStorageHeartbeatInterval, &c.HeartbeatInterval); err != nil {
		return err
	}
	if err := c.applyDurationEnv(EnvStorageRPCTimeout, &c.RPCTimeout); err != nil {
		return err
	}
	if err := c.applyDurationEnv(EnvStorageRetryBaseBackoff, &c.RetryBaseBackoff); err != nil {
		return err
	}
	if err := c.applyDurationEnv(EnvStorageRetryMaxBackoff, &c.RetryMaxBackoff); err != nil {
		return err
	}
	return nil
}

func (c *StorageNodeConfig) applyDurationEnv(key string, target *time.Duration) error {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err == nil {
		*target = d
		return nil
	}
	return fmt.Errorf("invalid env var %s: %w", key, err)
}

func (c *StorageNodeConfig) Default() {
	if c.GRPCAddr == "" {
		c.GRPCAddr = DefaultStorageGRPCAddr
	}
	if len(c.MetadataAddrs) == 0 {
		c.MetadataAddrs = []string{DefaultStorageMetadataAddr}
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
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = DefaultStorageRPCTimeout
	}
	if c.RetryMaxAttempts <= 0 {
		c.RetryMaxAttempts = DefaultStorageRetryMaxAttempts
	}
	if c.RetryBaseBackoff <= 0 {
		c.RetryBaseBackoff = DefaultStorageRetryBaseBackoff
	}
	if c.RetryMaxBackoff <= 0 {
		c.RetryMaxBackoff = DefaultStorageRetryMaxBackoff
	}
}

func (c *StorageNodeConfig) Validate() error {
	if c.GRPCAddr == "" {
		return errors.New("StorageNodeConfig: GRPCAddr must not be empty")
	}
	if len(c.MetadataAddrs) == 0 {
		return errors.New("StorageNodeConfig: MetadataAddrs must not be empty")
	}
	if c.DataDir == "" {
		return errors.New("StorageNodeConfig: DataDir must not be empty")
	}
	if c.ReplicationFactor < 1 {
		return errors.New("StorageNodeConfig: ReplicationFactor must be at least 1")
	}
	if c.RetryMaxAttempts < 1 {
		return fmt.Errorf("StorageNodeConfig: RetryMaxAttempts must be >= 1, got %d", c.RetryMaxAttempts)
	}
	if c.RetryBaseBackoff <= 0 {
		return fmt.Errorf("StorageNodeConfig: RetryBaseBackoff must be > 0, got %v", c.RetryBaseBackoff)
	}
	if c.RetryMaxBackoff < c.RetryBaseBackoff {
		return fmt.Errorf("StorageNodeConfig: RetryMaxBackoff (%v) must be >= RetryBaseBackoff (%v)",
			c.RetryMaxBackoff, c.RetryBaseBackoff)
	}
	if c.RPCTimeout <= 0 {
		return fmt.Errorf("StorageNodeConfig: RPCTimeout must be > 0, got %v", c.RPCTimeout)
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
