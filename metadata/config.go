// config.go
package metadata

import (
	"encoding/json"
	"errors"
	"os"
	"time"
)

const (
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
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var raw struct {
		NodeID            string            `json:"node_id"`
		GRPCAddr          string            `json:"grpc_addr"`
		RaftAddr          string            `json:"raft_addr"`
		RaftDir           string            `json:"raft_dir"`
		PeerAddrs         map[string]string `json:"peer_addrs"`
		Bootstrap         bool              `json:"bootstrap"`
		ReplicationFactor int               `json:"replication_factor"`
		SuspectTimeout    any               `json:"suspect_timeout"`
		DeadTimeout       any               `json:"dead_timeout"`
		WatcherInterval   any               `json:"watcher_interval"`
		ReconcileDelay    any               `json:"reconcile_delay"`
		HeartbeatTimeout  any               `json:"heartbeat_timeout"`
		ElectionTimeout   any               `json:"election_timeout"`
		SnapshotInterval  any               `json:"snapshot_interval"`
		SnapshotThreshold uint64            `json:"snapshot_threshold"`
		SnapshotRetain    int               `json:"snapshot_retain"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	cfg := &NodeConfig{
		NodeID:            raw.NodeID,
		GRPCAddr:          raw.GRPCAddr,
		RaftAddr:          raw.RaftAddr,
		RaftDir:           raw.RaftDir,
		PeerAddrs:         raw.PeerAddrs,
		Bootstrap:         raw.Bootstrap,
		ReplicationFactor: raw.ReplicationFactor,
		SnapshotThreshold: raw.SnapshotThreshold,
		SnapshotRetain:    raw.SnapshotRetain,
	}

	cfg.SuspectTimeout = parseDuration(raw.SuspectTimeout)
	cfg.DeadTimeout = parseDuration(raw.DeadTimeout)
	cfg.WatcherInterval = parseDuration(raw.WatcherInterval)
	cfg.ReconcileDelay = parseDuration(raw.ReconcileDelay)
	cfg.HeartbeatTimeout = parseDuration(raw.HeartbeatTimeout)
	cfg.ElectionTimeout = parseDuration(raw.ElectionTimeout)
	cfg.SnapshotInterval = parseDuration(raw.SnapshotInterval)

	cfg.Default()
	return cfg, nil
}

func parseDuration(v any) time.Duration {
	if v == nil {
		return 0
	}
	switch d := v.(type) {
	case string:
		if d == "" {
			return 0
		}
		pd, _ := time.ParseDuration(d)
		return pd
	case float64:
		return time.Duration(d)
	default:
		return 0
	}
}

func (c *NodeConfig) Default() {
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
