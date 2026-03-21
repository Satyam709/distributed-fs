package metadata

import (
	"fmt"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type RaftConfig struct {
	Config      NodeConfig
	FSM         raft.FSM
	LogStore    raft.LogStore
	StableStore raft.StableStore
}

func NewRaftNode(r RaftConfig) (*raft.Raft, error) {

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(r.Config.NodeID)
	raftConfig.HeartbeatTimeout = r.Config.HeartbeatTimeout
	raftConfig.ElectionTimeout = r.Config.ElectionTimeout
	raftConfig.SnapshotInterval = r.Config.SnapshotInterval
	raftConfig.SnapshotThreshold = r.Config.SnapshotThreshold

	addr, err := net.ResolveTCPAddr("tcp", r.Config.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}

	transport, err := raft.NewTCPTransport(
		r.Config.RaftAddr,
		addr,
		3,              // maxPool
		10*time.Second, // timeout
		nil,            // logger (nil = discard)
	)
	if err != nil {
		return nil, fmt.Errorf("create transport: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(
		r.Config.RaftDir,
		r.Config.SnapshotRetain,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	raftNode, err := raft.NewRaft(
		raftConfig,
		r.FSM,
		r.LogStore,
		r.StableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	// Bootstrap is the operator's responsibility —
	// set NodeConfig.Bootstrap = true only on first ever startup

	if r.Config.Bootstrap {
		servers := []raft.Server{
			{
				ID:      raft.ServerID(r.Config.NodeID),
				Address: raft.ServerAddress(r.Config.RaftAddr),
			},
		}

		// add peers if this is a multi-node cluster
		for id, addr := range r.Config.PeerAddrs {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(addr),
			})
		}

		cfg := raft.Configuration{Servers: servers}
		if err := raftNode.BootstrapCluster(cfg).Error(); err != nil {
			return nil, fmt.Errorf("bootstrap cluster: %w", err)
		}
	}

	return raftNode, nil
}

func IsLeader(r *raft.Raft) bool {
	return r.State() == raft.Leader
}

func LeaderAddress(r *raft.Raft) string {
	return string(r.Leader())
}

func WaitForLeader(r *raft.Raft, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr := r.Leader(); addr != "" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for raft leader")
}
