// Package watcher provides the NodeWatcher component, which runs on the
// Raft leader and periodically sweeps the NodeRegistry to detect storage
// nodes that have missed heartbeats. When a node's LastSeen exceeds the
// configured SuspectTimeout it is marked dead via a Raft-committed
// CmdMarkNodeDead command, and repair is triggered for its chunks.
package watcher

import (
	"encoding/json"
	"time"

	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
)

// Proposer abstracts the two Raft operations the watcher needs:
// checking leadership and proposing commands. This allows unit
// testing without a real Raft instance.
type Proposer interface {
	// IsLeader reports whether this node is currently the Raft leader.
	IsLeader() bool
	// Propose submits a MetadataCommand to the Raft cluster.
	Propose(cmd fsm.MetadataCommand) error
}

// RepairTriggerer is satisfied by the RepairScheduler (or a mock).
// It decouples the watcher from the scheduler's concrete type.
type RepairTriggerer interface {
	// TriggerRepair initiates re-replication for all chunks that had
	// a replica on the given (now dead) node.
	TriggerRepair(deadNodeID string)
}

// NodeWatcher periodically sweeps the NodeRegistry on the leader to
// detect dead storage nodes. It is safe for concurrent use.
type NodeWatcher struct {
	fsm            *fsm.MetadataFSM
	proposer       Proposer
	repair         RepairTriggerer
	suspectTimeout time.Duration
	interval       time.Duration
	logger         *logging.CLogger

	// stopCh is closed by Stop() to signal the sweep goroutine to exit.
	stopCh chan struct{}
}

// NewNodeWatcher constructs a NodeWatcher. Call Start() to begin sweeping.
func NewNodeWatcher(
	fsm *fsm.MetadataFSM,
	proposer Proposer,
	repair RepairTriggerer,
	suspectTimeout time.Duration,
	interval time.Duration,
	logger *logging.CLogger,
) *NodeWatcher {
	return &NodeWatcher{
		fsm:            fsm,
		proposer:       proposer,
		repair:         repair,
		suspectTimeout: suspectTimeout,
		interval:       interval,
		logger:         logger,
		stopCh:         make(chan struct{}),
	}
}

// Start launches the background sweep goroutine. It blocks until Stop()
// is called or the provided stopCh is closed.
func (nw *NodeWatcher) Start() {
	nw.logger.Info("NodeWatcher: starting", "interval", nw.interval, "suspectTimeout", nw.suspectTimeout)

	ticker := time.NewTicker(nw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nw.sweep()
		case <-nw.stopCh:
			nw.logger.Info("NodeWatcher: stopped")
			return
		}
	}
}

// Stop signals the sweep goroutine to exit. Safe to call multiple times.
func (nw *NodeWatcher) Stop() {
	select {
	case <-nw.stopCh:
		// already closed
	default:
		close(nw.stopCh)
	}
}

func (nw *NodeWatcher) UpdateLastSeen(nodeID string, seenAt time.Time) error {
	return nw.fsm.UpdateLastSeen(nodeID, seenAt)
}

// sweep iterates all nodes and marks stale alive nodes as dead.
// Only performs work when this node is the Raft leader.
func (nw *NodeWatcher) sweep() {
	if !nw.proposer.IsLeader() {
		nw.logger.Debug("NodeWatcher: sweep failed: Not leader")
		return
	}

	now := time.Now()
	nodes := nw.fsm.GetAllNodes()

	for _, node := range nodes {
		if node.Status != fsm.NodeStatusAlive {
			continue
		}

		elapsed := now.Sub(node.LastSeen)
		if elapsed <= nw.suspectTimeout {
			continue
		}

		nw.logger.Warn("NodeWatcher: node exceeded suspect timeout, marking dead",
			"node_id", node.NodeID,
			"last_seen", node.LastSeen,
			"elapsed", elapsed,
		)

		// Propose CmdMarkNodeDead through Raft.
		cmd, err := buildMarkDeadCmd(node.NodeID, now)
		if err != nil {
			nw.logger.Error("NodeWatcher: failed to build CmdMarkNodeDead", err, "node_id", node.NodeID)
			continue
		}

		if err := nw.proposer.Propose(cmd); err != nil {
			nw.logger.Error("NodeWatcher: failed to propose CmdMarkNodeDead", err, "node_id", node.NodeID)
			continue
		}

		// Trigger re-replication for chunks on the dead node.
		if nw.repair != nil {
			nw.repair.TriggerRepair(node.NodeID)
		}
	}
}

// buildMarkDeadCmd constructs a MetadataCommand of type CmdMarkNodeDead.
func buildMarkDeadCmd(nodeID string, now time.Time) (fsm.MetadataCommand, error) {
	payload, err := json.Marshal(fsm.CommandMarkNodeDead{
		NodeID:    nodeID,
		UpdatedAt: now,
	})
	if err != nil {
		return fsm.MetadataCommand{}, err
	}
	return fsm.MetadataCommand{
		Type:    fsm.CmdMarkNodeDead,
		Payload: payload,
	}, nil
}
