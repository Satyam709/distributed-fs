// config.go
package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultNodeID   = "node-1"
	DefaultGRPCAddr = ":4001"
	DefaultRaftAddr = "127.0.0.1:5001"
	DefaultRaftDir  = "./data/metadata/raft"

	DefaultReplicationFactor = 3
	DefaultSuspectTimeout    = 10 * time.Second
	DefaultDeadTimeout       = 30 * time.Second
	DefaultWatcherInterval   = 5 * time.Second
	DefaultReconcileDelay    = 30 * time.Second

	DefaultHeartbeatTimeout = 1 * time.Second
	DefaultElectionTimeout  = 5 * time.Second
	DefaultSnapshotInterval = 30 * time.Second

	DefaultSnapshotThreshold = 8192
	DefaultSnapshotRetain    = 2
)

const (
	EnvMetadataNodeID            string = "METADATA_NODE_ID"
	EnvMetadataGrpcAddr          string = "METADATA_GRPC_ADDR"
	EnvMetadataRaftAddr          string = "METADATA_RAFT_ADDR"
	EnvMetadataRaftDir           string = "METADATA_RAFT_DIR"
	EnvMetadataPeerAddrs         string = "METADATA_PEER_ADDRS"
	EnvMetadataBootstrap         string = "METADATA_BOOTSTRAP"
	EnvMetadataReplicationFactor string = "METADATA_REPLICATION_FACTOR"
	EnvMetadataJsonConfigPath    string = "METADATA_CONFIG"

	EnvMetadataSuspectTimeout    string = "METADATA_SUSPECT_TIMEOUT"
	EnvMetadataDeadTimeout       string = "METADATA_DEAD_TIMEOUT"
	EnvMetadataWatcherInterval   string = "METADATA_WATCHER_INTERVAL"
	EnvMetadataReconcileDelay    string = "METADATA_RECONCILE_DELAY"
	EnvMetadataHeartbeatTimeout  string = "METADATA_HEARTBEAT_TIMEOUT"
	EnvMetadataElectionTimeout   string = "METADATA_ELECTION_TIMEOUT"
	EnvMetadataSnapshotInterval  string = "METADATA_SNAPSHOT_INTERVAL"
	EnvMetadataSnapshotThreshold string = "METADATA_SNAPSHOT_THRESHOLD"
	EnvMetadataSnapshotRetain    string = "METADATA_SNAPSHOT_RETAIN"
)

type NodeConfig struct {
	NodeID    string            `json:"node_id"`
	GRPCAddr  string            `json:"grpc_addr"`
	RaftAddr  string            `json:"raft_addr"`
	RaftDir   string            `json:"raft_dir"`
	PeerAddrs map[string]string `json:"peer_addrs"` // nodeID → raftAddr

	Bootstrap bool `json:"bootstrap"`

	ReplicationFactor int           `json:"replication_factor"`
	SuspectTimeout    time.Duration `json:"suspect_timeout"`
	DeadTimeout       time.Duration `json:"dead_timeout"`
	WatcherInterval   time.Duration `json:"watcher_interval"`
	ReconcileDelay    time.Duration `json:"reconcile_delay"`

	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`
	ElectionTimeout  time.Duration `json:"election_timeout"`
	SnapshotInterval time.Duration `json:"snapshot_interval"`

	SnapshotThreshold uint64 `json:"snapshot_threshold"`
	SnapshotRetain    int    `json:"snapshot_retain"`
}

func DefaultNodeConfig() NodeConfig {
	return NodeConfig{
		NodeID:            DefaultNodeID,
		GRPCAddr:          DefaultGRPCAddr,
		RaftAddr:          DefaultRaftAddr,
		RaftDir:           DefaultRaftDir,
		ReplicationFactor: DefaultReplicationFactor,
		SuspectTimeout:    DefaultSuspectTimeout,
		DeadTimeout:       DefaultDeadTimeout,
		WatcherInterval:   DefaultWatcherInterval,
		ReconcileDelay:    DefaultReconcileDelay,
		HeartbeatTimeout:  DefaultHeartbeatTimeout,
		ElectionTimeout:   DefaultElectionTimeout,
		SnapshotInterval:  DefaultSnapshotInterval,
		SnapshotThreshold: DefaultSnapshotThreshold,
		SnapshotRetain:    DefaultSnapshotRetain,
	}
}

func (c *NodeConfig) Validate() error {
	if c.NodeID == "" {
		return errors.New("config: NodeID is required")
	}
	if c.GRPCAddr == "" {
		return errors.New("config: GRPCAddr is required")
	}
	if c.RaftAddr == "" {
		return errors.New("config: RaftAddr is required")
	}
	if c.RaftDir == "" {
		return errors.New("config: RaftDir is required")
	}
	return nil
}

func LoadFromJSON(path string) (*NodeConfig, error) {
	cfg := DefaultNodeConfig()
	if err := cfg.ApplyJSON(path); err != nil {
		return nil, err
	}
	cfg.Default()
	return &cfg, nil
}

func LoadConfigDefaultFlow() (NodeConfig, error) {
	cfg := DefaultNodeConfig()

	if configPath := os.Getenv(EnvMetadataJsonConfigPath); configPath != "" {
		if err := cfg.ApplyJSON(configPath); err != nil {
			return NodeConfig{}, fmt.Errorf("failed to load config from %s: %w", configPath, err)
		}
	}

	if err := cfg.ApplyEnv(); err != nil {
		return NodeConfig{}, fmt.Errorf("failed to apply env config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return NodeConfig{}, fmt.Errorf("failed to validate config: %w", err)
	}

	return cfg, nil
}

func (c *NodeConfig) ApplyJSON(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var raw struct {
		NodeID            *string           `json:"node_id"`
		GRPCAddr          *string           `json:"grpc_addr"`
		RaftAddr          *string           `json:"raft_addr"`
		RaftDir           *string           `json:"raft_dir"`
		PeerAddrs         map[string]string `json:"peer_addrs"`
		Bootstrap         *bool             `json:"bootstrap"`
		ReplicationFactor *int              `json:"replication_factor"`
		SuspectTimeout    json.RawMessage   `json:"suspect_timeout"`
		DeadTimeout       json.RawMessage   `json:"dead_timeout"`
		WatcherInterval   json.RawMessage   `json:"watcher_interval"`
		ReconcileDelay    json.RawMessage   `json:"reconcile_delay"`
		HeartbeatTimeout  json.RawMessage   `json:"heartbeat_timeout"`
		ElectionTimeout   json.RawMessage   `json:"election_timeout"`
		SnapshotInterval  json.RawMessage   `json:"snapshot_interval"`
		SnapshotThreshold *uint64           `json:"snapshot_threshold"`
		SnapshotRetain    *int              `json:"snapshot_retain"`
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
	if raw.RaftAddr != nil {
		c.RaftAddr = *raw.RaftAddr
	}
	if raw.RaftDir != nil {
		c.RaftDir = *raw.RaftDir
	}
	if raw.PeerAddrs != nil {
		c.PeerAddrs = raw.PeerAddrs
	}
	if raw.Bootstrap != nil {
		c.Bootstrap = *raw.Bootstrap
	}
	if raw.ReplicationFactor != nil {
		c.ReplicationFactor = *raw.ReplicationFactor
	}
	if raw.SnapshotThreshold != nil {
		c.SnapshotThreshold = *raw.SnapshotThreshold
	}
	if raw.SnapshotRetain != nil {
		c.SnapshotRetain = *raw.SnapshotRetain
	}

	if err := applyDurationJSON(raw.SuspectTimeout, &c.SuspectTimeout); err != nil {
		return fmt.Errorf("config: suspect_timeout: %w", err)
	}
	if err := applyDurationJSON(raw.DeadTimeout, &c.DeadTimeout); err != nil {
		return fmt.Errorf("config: dead_timeout: %w", err)
	}
	if err := applyDurationJSON(raw.WatcherInterval, &c.WatcherInterval); err != nil {
		return fmt.Errorf("config: watcher_interval: %w", err)
	}
	if err := applyDurationJSON(raw.ReconcileDelay, &c.ReconcileDelay); err != nil {
		return fmt.Errorf("config: reconcile_delay: %w", err)
	}
	if err := applyDurationJSON(raw.HeartbeatTimeout, &c.HeartbeatTimeout); err != nil {
		return fmt.Errorf("config: heartbeat_timeout: %w", err)
	}
	if err := applyDurationJSON(raw.ElectionTimeout, &c.ElectionTimeout); err != nil {
		return fmt.Errorf("config: election_timeout: %w", err)
	}
	if err := applyDurationJSON(raw.SnapshotInterval, &c.SnapshotInterval); err != nil {
		return fmt.Errorf("config: snapshot_interval: %w", err)
	}

	return nil
}

func (c *NodeConfig) ApplyEnv() error {
	if v := os.Getenv(EnvMetadataNodeID); v != "" {
		c.NodeID = v
	}
	if v := os.Getenv(EnvMetadataGrpcAddr); v != "" {
		c.GRPCAddr = v
	}
	if v := os.Getenv(EnvMetadataRaftAddr); v != "" {
		c.RaftAddr = v
	}
	if v := os.Getenv(EnvMetadataRaftDir); v != "" {
		c.RaftDir = v
	}
	if v := os.Getenv(EnvMetadataPeerAddrs); v != "" {
		c.PeerAddrs = parsePeerAddrs(v)
	}
	if v := os.Getenv(EnvMetadataBootstrap); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid env var %s: %w", EnvMetadataBootstrap, err)
		}
		c.Bootstrap = parsed
	}
	if v := os.Getenv(EnvMetadataReplicationFactor); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid env var %s: %w", EnvMetadataReplicationFactor, err)
		}
		c.ReplicationFactor = parsed
	}
	if err := applyDurationEnv(EnvMetadataSuspectTimeout, &c.SuspectTimeout); err != nil {
		return err
	}
	if err := applyDurationEnv(EnvMetadataDeadTimeout, &c.DeadTimeout); err != nil {
		return err
	}
	if err := applyDurationEnv(EnvMetadataWatcherInterval, &c.WatcherInterval); err != nil {
		return err
	}
	if err := applyDurationEnv(EnvMetadataReconcileDelay, &c.ReconcileDelay); err != nil {
		return err
	}
	if err := applyDurationEnv(EnvMetadataHeartbeatTimeout, &c.HeartbeatTimeout); err != nil {
		return err
	}
	if err := applyDurationEnv(EnvMetadataElectionTimeout, &c.ElectionTimeout); err != nil {
		return err
	}
	if err := applyDurationEnv(EnvMetadataSnapshotInterval, &c.SnapshotInterval); err != nil {
		return err
	}
	if v := os.Getenv(EnvMetadataSnapshotThreshold); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid env var %s: %w", EnvMetadataSnapshotThreshold, err)
		}
		c.SnapshotThreshold = parsed
	}
	if v := os.Getenv(EnvMetadataSnapshotRetain); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid env var %s: %w", EnvMetadataSnapshotRetain, err)
		}
		c.SnapshotRetain = parsed
	}
	return nil
}

func applyDurationJSON(raw json.RawMessage, target *time.Duration) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var asString string
	if err := json.Unmarshal(raw, &asString); err != nil {
		return fmt.Errorf("invalid duration format: %w", err)
	}

	d, err := time.ParseDuration(asString)
	if err != nil {
		return fmt.Errorf("invalid duration value: %w", err)
	}

	*target = d
	return nil
}

func applyDurationEnv(key string, target *time.Duration) error {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fmt.Errorf("invalid env var %s: %w", key, err)
	}
	*target = d
	return nil
}

func parsePeerAddrs(s string) map[string]string {
	if s == "" {
		return nil
	}

	result := make(map[string]string)
	for pair := range strings.SplitSeq(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}
		nodeID := strings.TrimSpace(parts[0])
		addr := strings.TrimSpace(parts[1])
		if nodeID == "" || addr == "" {
			continue
		}
		result[nodeID] = addr
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func (c *NodeConfig) Default() {
	if c.NodeID == "" {
		c.NodeID = DefaultNodeID
	}
	if c.GRPCAddr == "" {
		c.GRPCAddr = DefaultGRPCAddr
	}
	if c.RaftAddr == "" {
		c.RaftAddr = DefaultRaftAddr
	}
	if c.RaftDir == "" {
		c.RaftDir = DefaultRaftDir
	}
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = DefaultReplicationFactor
	}
	if c.SuspectTimeout <= 0 {
		c.SuspectTimeout = DefaultSuspectTimeout
	}
	if c.DeadTimeout <= 0 {
		c.DeadTimeout = DefaultDeadTimeout
	}
	if c.WatcherInterval <= 0 {
		c.WatcherInterval = DefaultWatcherInterval
	}
	if c.ReconcileDelay <= 0 {
		c.ReconcileDelay = DefaultReconcileDelay
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = DefaultElectionTimeout
	}
	if c.SnapshotInterval <= 0 {
		c.SnapshotInterval = DefaultSnapshotInterval
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = DefaultSnapshotThreshold
	}
	if c.SnapshotRetain <= 0 {
		c.SnapshotRetain = DefaultSnapshotRetain
	}
}

func (c *NodeConfig) ReplicationCount() int {
	if c.ReplicationFactor <= 0 {
		return DefaultReplicationFactor
	}
	return c.ReplicationFactor
}
