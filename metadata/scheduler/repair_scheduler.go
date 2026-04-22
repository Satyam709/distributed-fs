package scheduler

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"github.com/satyam709/distributed-fs/metadata/placement"
)

const (
	workerCount              = 5 // count of max concurrent workers
	defaultReplicationFactor = 3
	maxReplicationFactor     = 10
	repairQueueCapacity      = 1024 // buffered job channel to prevent blocking
)

type RepairScheduler struct {
	fsm               *fsm.MetadataFSM
	propose           func(fsm.MetadataCommand) error
	mutex             *sync.Mutex
	pendingJobs       map[string][]string // nodeID → list of pending job IDs
	jobs              chan string         // job IDs to process
	replicationFactor int
	sourceStrategy    placement.PlacementStrategy // selects which live replica to read from
	targetStrategy    placement.PlacementStrategy // selects which nodes to replicate to / remove from
	logger            *logging.CLogger
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
}

// Start launches repair worker goroutines consuming an internal job channel
// and a recovery goroutine that runs once on startup to recover stuck
// in-progress jobs.
func (rs *RepairScheduler) Start(ctx context.Context) {
	rs.ctx, rs.cancel = context.WithCancel(ctx)

	rs.wg.Add(2)
	go func() {
		defer rs.wg.Done()
		rs.spawnWorkers(rs.ctx, workerCount)
	}()
	go func() {
		defer rs.wg.Done()
		rs.RecoverStuckJobs(rs.ctx)
	}()
}

// Stop cancels the context and waits for all workers to drain.
func (rs *RepairScheduler) Stop() {
	if rs.cancel != nil {
		rs.cancel()
	}
	rs.wg.Wait()
}

func (rs *RepairScheduler) spawnWorkers(ctx context.Context, count int) {
	wg := sync.WaitGroup{}
	wg.Add(count)

	for range count {
		go func(ctx context.Context) {
			defer wg.Done()
			for {
				select {
				case jobID := <-rs.jobs:
					rs.executeJob(jobID)
				case <-ctx.Done():
					return
				}
			}
		}(ctx)
	}

	wg.Wait()
}

// TriggerRepair is called by NodeWatcher when a node is marked dead.
// Scans all chunks that had a replica on the dead node via
// fsm.GetChunksByNode(deadNodeID). For each chunk, computes current live
// replica count. If below replication factor, calculates deficit and calls
// scheduleRepairJobs for each missing replica.
func (rs *RepairScheduler) TriggerRepair(deadNodeID string) {
	chunkIDs, err := rs.fsm.GetChunksByNode(deadNodeID)
	if err != nil {
		rs.logger.Error("cannot get chunks for dead node", err, "nodeID", deadNodeID)
		return
	}

	for _, chkId := range chunkIDs {
		liveReplicas, err := rs.fsm.GetChunkLocations(chkId)
		if err != nil {
			rs.logger.Error("cannot trigger repair", err, "chunkID", chkId)
			continue
		}
		deficit := rs.replicationFactor - len(liveReplicas)
		if deficit <= 0 {
			continue
		}
		rs.scheduleRepairJobs(chkId, liveReplicas, deficit)
	}
}

// scheduleRepairJobs uses sourceStrategy to pick the best source node
// and targetStrategy to pick target nodes. Proposes CmdCreateRepairJob
// through Raft. Enqueues to internal job channel (non-blocking to
// prevent deadlocks).
func (rs *RepairScheduler) scheduleRepairJobs(chunkID string, liveReplicas []fsm.NodeEntry, deficit int) {
	if deficit > 0 {
		// under-replicated
		if len(liveReplicas) == 0 {
			rs.logger.Warn("cannot schedule repair with no live replicas", "chunkID", chunkID)
			return
		}

		// Fetch all live nodes as candidates; the strategy will exclude current replicas
		liveNodes, err := rs.fsm.GetLiveNodes()
		if err != nil {
			rs.logger.Error("cannot get live nodes for placement", err, "chunkID", chunkID)
			return
		}

		newNodes, err := rs.targetStrategy.SelectNodes(liveNodes, chunkID, deficit, liveReplicas...)
		if err != nil {
			rs.logger.Error("scheduling failed", err, "chunkID", chunkID)
			return
		}

		// Delegate source selection to the source strategy
		source := rs.sourceStrategy.SelectPrimary(liveReplicas)
		sourceNode := source.NodeID

		for _, node := range newNodes {
			jid := uuid.NewString()
			cmd := fsm.CommandCreateRepairJob{
				JobID:      jid,
				CreatedAt:  time.Now(),
				ChunkID:    chunkID,
				SourceNode: sourceNode,
				TargetNode: node.NodeID,
			}

			payload, err := json.Marshal(cmd)
			if err != nil {
				rs.logger.Error("scheduling failed", err, "chunkID", chunkID)
				continue
			}
			err = rs.propose(fsm.MetadataCommand{
				Type:    fsm.CmdCreateRepairJob,
				Payload: payload,
			})
			if err != nil {
				rs.logger.Error("schedule proposing failed", err, "chunkID", chunkID)
				continue
			}

			select {
			case rs.jobs <- jid:
			default:
				rs.logger.Warn("repair job queue full, dropping job", "jobID", jid, "chunkID", chunkID)
			}
		}
	} else {
		// over-replicated
		// Pass the chunk's live replicas; the target strategy picks which to remove
		deletingNodes, err := rs.targetStrategy.SelectNodeReverse(liveReplicas, chunkID, -deficit)
		if err != nil {
			rs.logger.Error("scheduling failed", err, "chunkID", chunkID)
			return
		}
		for _, node := range deletingNodes {
			jid := uuid.NewString()
			cmd := fsm.CommandCreateRepairJob{
				JobID:        jid,
				CreatedAt:    time.Now(),
				ChunkID:      chunkID,
				DeleteSource: true,
				SourceNode:   node.NodeID,
			}

			payload, err := json.Marshal(cmd)
			if err != nil {
				rs.logger.Error("scheduling failed", err, "chunkID", chunkID)
				continue
			}
			err = rs.propose(fsm.MetadataCommand{
				Type:    fsm.CmdCreateRepairJob,
				Payload: payload,
			})
			if err != nil {
				rs.logger.Error("schedule proposing failed", err, "chunkID", chunkID)
				continue
			}

			// TODO: review this drop of job and its retry
			select {
			case rs.jobs <- jid:
			default:
				rs.logger.Warn("repair job queue full, dropping job", "jobID", jid, "chunkID", chunkID)
			}
		}
	}
}

// executeJob looks up the repair job by ID from the FSM and delivers
// the repair instruction to the source node. Repair instructions are
// piggybacked on heartbeat responses — so this method adds the job ID
// to a pending map keyed by source node ID. When that node next
// heartbeats, the handler includes the pending job in the response.
func (rs *RepairScheduler) executeJob(jobID string) {
	job, err := rs.fsm.GetRepairJob(jobID)
	if err != nil {
		rs.logger.Error("job lookup failed", err, "jobId", jobID)
		return
	}

	rs.mutex.Lock()
	rs.pendingJobs[job.SourceNodeID] = appendUnique(rs.pendingJobs[job.SourceNodeID], jobID)
	rs.mutex.Unlock()

	// mark this job as INPROGRESS
	updateCmd := fsm.CommandUpdateRepairJob{
		JobID:     jobID,
		Status:    fsm.RepairStatusInProgress,
		UpdatedAt: time.Now(),
	}
	payload, err := json.Marshal(updateCmd)
	if err != nil {
		rs.logger.Error("job execution failed", err, "jobId", jobID)
		return
	}
	err = rs.propose(fsm.MetadataCommand{
		Type:    fsm.CmdUpdateRepairJob,
		Payload: payload,
	})
	if err != nil {
		rs.logger.Error("job execution failed", err, "jobId", jobID)
	}
}

// RecoverStuckJobs scans all jobs with status InProgress via
// fsm.GetJobsByStatus(InProgress). For each, checks if source node is
// still alive. If not, resets job to pending and re-enqueues with a new
// source node.
func (rs *RepairScheduler) RecoverStuckJobs(ctx context.Context) {
	rs.logger.Info("recovering the pending jobs...")
	stuckJobs, err := rs.fsm.GetJobsByStatus(fsm.RepairStatusInProgress)
	if err != nil {
		rs.logger.Error("failed to get stuckjobs", err)
		return
	}
	for _, val := range stuckJobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		liveReplicas, err := rs.fsm.GetChunkLocations(val.ChunkID)
		if err != nil {
			rs.logger.Error("cannot trigger repair", err, "chunkID", val.ChunkID)
			continue
		}
		if len(liveReplicas) == 0 {
			rs.logger.Warn("cannot recover repair job with no live replicas", "jobID", val.JobID, "chunkID", val.ChunkID)
			continue
		}

		deficit := rs.replicationFactor - len(liveReplicas)
		if deficit <= 0 {
			continue
		}

		sourceNodeStillAlive := false
		for _, nid := range liveReplicas {
			if val.SourceNodeID == nid.NodeID {
				sourceNodeStillAlive = true
				break
			}
		}

		if sourceNodeStillAlive {
			if !rs.enqueueJob(ctx, val.JobID) {
				return
			}
			continue
		}

		newSource := rs.sourceStrategy.SelectPrimary(liveReplicas)
		cmd := fsm.CommandUpdateRepairJob{
			JobID:      val.JobID,
			SourceNode: newSource.NodeID,
			Status:     fsm.RepairStatusPending,
			UpdatedAt:  time.Now(),
		}
		payload, err := json.Marshal(cmd)
		if err != nil {
			rs.logger.Error("scheduling failed", err, "chunkID", val.ChunkID)
			continue
		}
		err = rs.propose(fsm.MetadataCommand{
			Type:    fsm.CmdUpdateRepairJob,
			Payload: payload,
		})
		if err != nil {
			rs.logger.Error("schedule proposing failed", err, "chunkID", val.ChunkID)
			continue
		}
		if !rs.enqueueJob(ctx, val.JobID) {
			return
		}
	}
}

func (rs *RepairScheduler) GetPendingJobsForNode(nodeID string) []string {
	rs.mutex.Lock()
	defer rs.mutex.Unlock()

	jobs, ok := rs.pendingJobs[nodeID]
	if !ok {
		return nil
	}
	return append([]string(nil), jobs...)
}

// OnJobComplete is called by gRPC handler when a storage node reports
// repair outcome. Removes the job from the pending map. On success,
// proposes the appropriate chunk replica update through Raft. On failure,
// re-schedules if the chunk is still under-replicated.
func (rs *RepairScheduler) OnJobComplete(nodeid, jobID string, success bool, errorMsg string) error {
	rs.mutex.Lock()
	if jobsForNode, ok := rs.pendingJobs[nodeid]; ok {
		removed := removeJob(jobsForNode, jobID)
		if len(removed) == 0 {
			delete(rs.pendingJobs, nodeid)
		} else {
			rs.pendingJobs[nodeid] = removed
		}
	}
	rs.mutex.Unlock()

	job, err := rs.fsm.GetRepairJob(jobID)
	if err != nil {
		return err
	}

	if success {
		if job.DeleteSource {
			// Over-replicated repair: evict the source node's replica
			return rs.proposeEvictChunk(job.ChunkID, job.SourceNodeID, "over-replicated repair completed")
		}
		// Under-replicated repair: register the new replica on the target node
		return rs.proposeAddReplica(job.ChunkID, job.TargetNodeID)
	}

	liveReplicas, err := rs.fsm.GetChunkLocations(job.ChunkID)
	if err != nil {
		return err
	}

	deficit := rs.replicationFactor - len(liveReplicas)
	if deficit > 0 {
		rs.logger.Warn("repair job failed, rescheduling", "jobID", jobID, "chunkID", job.ChunkID, "err", errorMsg)
		rs.scheduleRepairJobs(job.ChunkID, liveReplicas, deficit)
	}

	return nil
}

// proposeAddReplica submits a CmdAddChunkReplica through Raft to register
// a new replica node for the given chunk.
func (rs *RepairScheduler) proposeAddReplica(chunkID, nodeID string) error {
	cmd := fsm.CommandAddChunkReplica{ChunkID: chunkID, NodeID: nodeID}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return rs.propose(fsm.MetadataCommand{
		Type:    fsm.CmdAddChunkReplica,
		Payload: payload,
	})
}

// proposeEvictChunk submits a CmdEvictChunkFromNode through Raft to remove
// a node from the given chunk's replica list.
func (rs *RepairScheduler) proposeEvictChunk(chunkID, nodeID, reason string) error {
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
	return rs.propose(fsm.MetadataCommand{
		Type:    fsm.CmdEvictChunkFromNode,
		Payload: payload,
	})
}

func (rs *RepairScheduler) enqueueJob(ctx context.Context, jobID string) bool {
	select {
	case <-ctx.Done():
		return false
	case rs.jobs <- jobID:
		return true
	}
}

func appendUnique(values []string, v string) []string {
	for _, existing := range values {
		if existing == v {
			return values
		}
	}
	return append(values, v)
}

func removeJob(values []string, target string) []string {
	for i, v := range values {
		if v == target {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func NewRepairScheduler(r *raft.Raft, mfsm *fsm.MetadataFSM, source, target placement.PlacementStrategy, rf int) *RepairScheduler {
	newScheduler := &RepairScheduler{
		fsm: mfsm,
		propose: func(cmd fsm.MetadataCommand) error {
			return fsm.Propose(r, cmd)
		},
		jobs:           make(chan string, repairQueueCapacity),
		mutex:          &sync.Mutex{},
		pendingJobs:    map[string][]string{},
		sourceStrategy: source,
		targetStrategy: target,
		logger:         logging.NewCLogger().With("component", "RepairScheduler"),
	}
	if rf <= 0 {
		rf = defaultReplicationFactor
	}
	newScheduler.replicationFactor = min(rf, maxReplicationFactor)
	return newScheduler
}

// newTestableScheduler creates a RepairScheduler with a custom propose
// function for testing. Not exported — used only by tests in this package.
func newTestableScheduler(mfsm *fsm.MetadataFSM, source, target placement.PlacementStrategy, rf int, proposeFn func(fsm.MetadataCommand) error) *RepairScheduler {
	rs := &RepairScheduler{
		fsm:            mfsm,
		propose:        proposeFn,
		jobs:           make(chan string, repairQueueCapacity),
		mutex:          &sync.Mutex{},
		pendingJobs:    map[string][]string{},
		sourceStrategy: source,
		targetStrategy: target,
		logger:         logging.NewCLogger().With("component", "RepairScheduler"),
	}
	if rf <= 0 {
		rf = defaultReplicationFactor
	}
	rs.replicationFactor = min(rf, maxReplicationFactor)
	return rs
}
