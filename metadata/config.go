// config.go
package metadata

import "time"

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

	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"` // added
	ElectionTimeout  time.Duration `json:"election_timeout"`  // added
	SnapshotInterval time.Duration `json:"snapshot_interval"` // added

	SnapshotThreshold uint64 `json:"snapshot_threshold"`
	SnapshotRetain    int    `json:"snapshot_retain"`
}
