// Package reconcile implements the Reconciler component which handles
// storage-node restarts by comparing the node's reported chunk inventory
// against the canonical replica list in the metadata FSM.
package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CommandProposer is the subset of Raft operations the reconciler needs.
type CommandProposer interface {
	Propose(cmd fsm.MetadataCommand) error
}

// RepairTriggerer is satisfied by the RepairScheduler (or a mock).
type RepairTriggerer interface {
	ScheduleRepairForChunk(chunkID string)
}

// FSMReader provides read-only access to the FSM state needed for reconciliation.
type FSMReader interface {
	GetNode(nodeID string) (*fsm.NodeEntry, error)
	GetChunksByNode(nodeID string) ([]string, error)
	GetChunkLocations(chunkID string) ([]fsm.NodeEntry, error)
}

// StorageClient is the thin gRPC client used to send eviction instructions
// to storage nodes. This is the only place metadata opens connections to
// storage nodes.
type StorageClient interface {
	DeleteChunk(ctx context.Context, addr, chunkID string) error
}

// Reconciler compares a node's reported chunk inventory against the
// canonical FSM state and resolves discrepancies.
type Reconciler struct {
	fsm               FSMReader
	proposer          CommandProposer
	repairer          RepairTriggerer
	storageClient     StorageClient
	reconcileDelay    time.Duration
	replicationFactor int
	logger            *logging.CLogger

	mu     sync.Mutex
	timers map[string]*time.Timer
}

// NewReconciler constructs a Reconciler. delay is the time to wait after
// node registration before running reconciliation. rf is the target
// replication factor; if <= 0 it defaults to 3.
func NewReconciler(
	fsm FSMReader,
	proposer CommandProposer,
	repairer RepairTriggerer,
	storageClient StorageClient,
	delay time.Duration,
	rf int,
	logger *logging.CLogger,
) *Reconciler {
	if logger == nil {
		logger = logging.NewCLogger()
	}
	if rf <= 0 {
		rf = 3
	}
	return &Reconciler{
		fsm:               fsm,
		proposer:          proposer,
		repairer:          repairer,
		storageClient:     storageClient,
		reconcileDelay:    delay,
		replicationFactor: rf,
		logger:            logger.With(slog.String("component", "Reconciler")),
		timers:            make(map[string]*time.Timer),
	}
}

// Schedule starts a delayed reconciliation for the given node. If a timer
// for this node already exists it is stopped and replaced. The chunk list
// is captured at call time (the node reports its inventory during
// RegisterNode).
func (r *Reconciler) Schedule(nodeID string, reportedChunks []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if old, ok := r.timers[nodeID]; ok {
		stopped := old.Stop()
		if stopped {
			r.logger.Debug("cancelled previous reconciliation timer", slog.String("nodeID", nodeID))
		}
	}

	reported := make([]string, len(reportedChunks))
	copy(reported, reportedChunks)

	r.timers[nodeID] = time.AfterFunc(r.reconcileDelay, func() {
		r.ReconcileNode(nodeID, reported)

		r.mu.Lock()
		delete(r.timers, nodeID)
		r.mu.Unlock()
	})

	r.logger.Info("scheduled reconciliation", slog.String("nodeID", nodeID), slog.Duration("delay", r.reconcileDelay))
}

// Cancel stops any pending reconciliation timer for the node.
func (r *Reconciler) Cancel(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if old, ok := r.timers[nodeID]; ok {
		old.Stop()
		delete(r.timers, nodeID)
		r.logger.Debug("cancelled reconciliation timer", slog.String("nodeID", nodeID))
	}
}

// ReconcileNode compares the node's reported inventory against the
// canonical FSM state and resolves discrepancies.
//
// Two cases:
//  1. Node has chunk but FSM does not expect it there → stale replica.
//     Evict from FSM and send DeleteChunk RPC to node.
//  2. FSM expects chunk on node but node does not have it → missing replica.
//     Evict from FSM (cleans stale record) and schedule repair if now
//     under-replicated.
func (r *Reconciler) ReconcileNode(nodeID string, reportedChunks []string) {
	r.logger.Info("starting reconciliation", slog.String("nodeID", nodeID), slog.Int("reportedChunks", len(reportedChunks)))

	node, err := r.fsm.GetNode(nodeID)
	if err != nil {
		r.logger.Warn("reconciliation aborted: node not found in FSM", slog.String("nodeID", nodeID))
		return
	}

	expectedChunks, err := r.fsm.GetChunksByNode(nodeID)
	if err != nil {
		r.logger.Warn("reconciliation aborted: cannot read expected chunks", slog.String("nodeID", nodeID), slog.Any("err", err))
		return
	}

	reportedSet := make(map[string]struct{}, len(reportedChunks))
	for _, cid := range reportedChunks {
		reportedSet[cid] = struct{}{}
	}

	expectedSet := make(map[string]struct{}, len(expectedChunks))
	for _, cid := range expectedChunks {
		expectedSet[cid] = struct{}{}
	}

	// Case 1: stale replicas — node has them, FSM does not expect them.
	for _, cid := range reportedChunks {
		if _, expected := expectedSet[cid]; expected {
			continue
		}
		r.evictStaleReplica(nodeID, node.Address, cid)
	}

	// Case 2: missing replicas — FSM expects them, node does not have them.
	for _, cid := range expectedChunks {
		if _, reported := reportedSet[cid]; reported {
			continue
		}
		r.handleMissingReplica(nodeID, cid)
	}

	r.logger.Info("reconciliation complete", slog.String("nodeID", nodeID))
}

// evictStaleReplica removes a chunk from the FSM's replica list for this
// node and instructs the storage node to delete it.
func (r *Reconciler) evictStaleReplica(nodeID, nodeAddr, chunkID string) {
	r.logger.Info("evicting stale replica", slog.String("chunkID", chunkID), slog.String("nodeID", nodeID))

	// Propose eviction. If the chunk is unknown to the FSM (common for
	// stale replicas) the proposal will fail — we still send the DeleteChunk
	// RPC to clean up the storage node.
	if err := r.proposeEvictChunk(chunkID, nodeID, "stale replica during reconciliation"); err != nil {
		r.logger.Warn("FSM eviction failed for stale replica (chunk may not be known to FSM)", slog.String("chunkID", chunkID), slog.String("nodeID", nodeID), slog.Any("err", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := r.storageClient.DeleteChunk(ctx, nodeAddr, chunkID); err != nil {
		r.logger.Error("DeleteChunk RPC failed for stale replica", err, slog.String("chunkID", chunkID), slog.String("nodeAddr", nodeAddr))
		// We do not return here — the FSM eviction is already proposed and
		// the RPC failure is logged. A subsequent reconciliation or repair
		// will clean up if needed.
	}
}

// handleMissingReplica removes the stale FSM record for a chunk on this
// node and schedules repair if the chunk becomes under-replicated.
func (r *Reconciler) handleMissingReplica(nodeID, chunkID string) {
	r.logger.Info("handling missing replica", slog.String("chunkID", chunkID), slog.String("nodeID", nodeID))

	if err := r.proposeEvictChunk(chunkID, nodeID, "missing replica during reconciliation"); err != nil {
		r.logger.Error("failed to propose eviction for missing replica", err, slog.String("chunkID", chunkID), slog.String("nodeID", nodeID))
		return
	}

	liveReplicas, err := r.fsm.GetChunkLocations(chunkID)
	if err != nil {
		r.logger.Warn("cannot check replication status after missing replica", slog.String("chunkID", chunkID), slog.Any("err", err))
		return
	}

	// The eviction we just proposed may or may not have been applied to
	// the FSM yet (depends on whether the proposer is synchronous). We
	// check whether this node is still present in the live replica list
	// and adjust the expected count accordingly.
	currentLiveCount := len(liveReplicas)
	stillListed := false
	for _, n := range liveReplicas {
		if n.NodeID == nodeID {
			stillListed = true
			break
		}
	}
	expectedLiveCount := currentLiveCount
	if stillListed {
		expectedLiveCount = currentLiveCount - 1
	}

	if expectedLiveCount < r.replicationFactor {
		// There is a narrow race window: if the FSM eviction has not been
		// applied when the scheduler reads it, the scheduler may see the
		// old replica count and decide no repair is needed. The scheduler
		// worker runs asynchronously, so in practice the eviction is usually
		// applied by then. If not, a subsequent periodic scan (or manual
		// intervention) would catch it.
		r.repairer.ScheduleRepairForChunk(chunkID)
	}
}

func (r *Reconciler) proposeEvictChunk(chunkID, nodeID, reason string) error {
	cmd := fsm.CommandEvictChunkFromNode{
		ChunkID:   chunkID,
		NodeID:    nodeID,
		EvictedAt: time.Now(),
		Reason:    reason,
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return r.proposer.Propose(fsm.MetadataCommand{
		Type:    fsm.CmdEvictChunkFromNode,
		Payload: payload,
	})
}

// ---------------------------------------------------------------------------
// Thin gRPC storage client
// ---------------------------------------------------------------------------

// ThinStorageClient implements StorageClient using a simple per-RPC dial.
// It is intentionally lightweight — reconciliation is rare so connection
// pooling is unnecessary.
type ThinStorageClient struct{}

// NewThinStorageClient creates a new ThinStorageClient.
func NewThinStorageClient() *ThinStorageClient {
	return &ThinStorageClient{}
}

// DeleteChunk dials the storage node at addr and asks it to delete chunkID.
func (c *ThinStorageClient) DeleteChunk(ctx context.Context, addr, chunkID string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	client := pb_storage.NewStorageServiceClient(conn)
	stream, err := client.DeleteChunk(ctx, &pb_storage.DeleteChunkRequest{ChunkId: chunkID})
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if resp.GetResponse() != nil && resp.GetResponse().GetError() != "" {
			return errors.New(resp.GetResponse().GetError())
		}
		if !resp.GetSuccess() {
			return errors.New("storage node reported delete failure")
		}
	}
}
