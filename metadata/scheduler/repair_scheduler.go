package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
)

const (
	WORKER_COUNT               = 5 // count of max concurrent workers
	DEFAULT_REPLICATION_FACTOR = 3
	MAX_REPLICATION_FACTOR     = 10
)

type RepairScheduler struct {
	fsm               *fsm.MetadataFSM
	raft              *raft.Raft
	mutex             *sync.Mutex
	pendingJobs       map[string][]string // nodeID → list of pending job IDs
	jobs              chan string         // job IDs to process
	replicationFactor int
	placementStrategy PlacementStrategy
	logger            *logging.CLogger
}

// Start repair worker goroutines consuming an internal job channel. Also start a recovery goroutine that runs once on startup to recover stuck in-progress jobs.
func (rs *RepairScheduler) Start(ctx context.Context) {

	// spawn workers
	go rs.spawnWorkers(ctx, WORKER_COUNT)

	// run on startup
	// TODO: make this ctx aware
	go rs.RecoverStuckJobs(ctx)
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

// TriggerRepair is called by NodeWatcher when a node is marked dead. Scans all chunks that had a replica on the dead node via fsm.GetChunksByNode(deadNodeID). For each chunk, computes current live replica count. If below replication factor, calculates deficit and calls scheduleRepairJobs for each missing replica.
func (rs *RepairScheduler) TriggerRepair(deadNodeID string) error {
	chunkIDs, err := rs.fsm.GetChunksByNode(deadNodeID)
	if err != nil {
		return err
	}

	for _, chkId := range chunkIDs {
		liveReplicas, err := rs.fsm.GetChunkLocations(chkId)
		if err != nil {
			rs.logger.Error("cannot trigger repair", err, "chunkID", chkId)
		}
		deficit := rs.replicationFactor - len(liveReplicas)
		if deficit == 0 {
			continue
		}
		rs.scheduleRepairJobs(chkId, liveReplicas, deficit)
	}

	return nil
}

// scheduleRepairJobs picks a source node (live replica with lowest load) and a target node (PlacementStrategy — most free space, not already holding this chunk). Proposes CmdCreateRepairJob through Raft. Enqueues to internal job channel.
func (rs *RepairScheduler) scheduleRepairJobs(chunkID string, liveReplicas []fsm.NodeEntry, deficit int) {
	// underreplicated
	if deficit > 0 {
		newNodes, err := rs.placementStrategy.SelectNodes(chunkID, deficit, liveReplicas...)
		if err != nil {
			rs.logger.Error("scheduling failed", err, "chunkID", chunkID)
		}
		for _, node := range newNodes {
			jid := uuid.NewString()
			cmd := fsm.CommandCreateRepairJob{
				JobID:      jid,
				CreatedAt:  time.Now(),
				ChunkID:    chunkID,
				SourceNode: liveReplicas[0].NodeID,
				TargetNode: node.NodeID,
			}

			payload, err := json.Marshal(cmd)
			if err != nil {
				rs.logger.Error("scheduling failed", err, "chunkID", chunkID)
				continue
			}
			err = fsm.Propose(rs.raft, fsm.MetadataCommand{
				Type:    fsm.CmdCreateRepairJob,
				Payload: payload,
			})
			if err != nil {
				rs.logger.Error("schedule propsing failed", err, "chunkID", chunkID)
				continue
			}

			rs.jobs <- jid
		}
	} else {
		// overreplicated
		deletingNodes, err := rs.placementStrategy.SelectNodeReverse(chunkID, deficit)
		if err != nil {
			rs.logger.Error("scheduling failed", err, "chunkID", chunkID)
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
			err = fsm.Propose(rs.raft, fsm.MetadataCommand{
				Type:    fsm.CmdCreateRepairJob,
				Payload: payload,
			})
			if err != nil {
				rs.logger.Error("schedule propsing failed", err, "chunkID", chunkID)
				continue
			}
			rs.jobs <- jid
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
	defer rs.mutex.Unlock()
	rs.pendingJobs[job.SourceNodeID] = append(rs.pendingJobs[job.SourceNodeID], jobID)

	// mark this job as INPROGRESS
	err = rs.fsm.UpdateRepairJobStatus(rs.raft, jobID, fsm.RepairStatusInProgress)
	if err != nil {
		rs.logger.Error("job execution failed", err, "jobId", jobID)
	}
}

// RecoverStuckJobs scans all jobs with status InProgress via fsm.GetJobsByStatus(InProgress). For each, checks if source node is still alive. If not, resets job to pending and re-enqueues with a new source node.
func (rs *RepairScheduler) RecoverStuckJobs(ctx context.Context) {
	rs.logger.Info("recovering the pending jobs...")
	stuckJobs, err := rs.fsm.GetJobsByStatus(fsm.RepairStatusInProgress)
	if err != nil {
		rs.logger.Error("failed to get stuckjobs", err)
	}
	for _, val := range stuckJobs {
		liveReplicas, err := rs.fsm.GetChunkLocations(val.ChunkID)
		if err != nil {
			rs.logger.Error("cannot trigger repair", err, "chunkID")
		}
		deficit := rs.replicationFactor - len(liveReplicas)
		if deficit == 0 {
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
			rs.jobs <- val.JobID
			return
		}

		cmd := fsm.CommandUpdateRepairJob{
			JobID:      val.JobID,
			SourceNode: liveReplicas[0].NodeID,
			Status:     fsm.RepairStatusPending,
			UpdatedAt:  time.Now(),
		}
		payload, err := json.Marshal(cmd)
		if err != nil {
			rs.logger.Error("scheduling failed", err, "chunkID", val.ChunkID)
			continue
		}
		err = fsm.Propose(rs.raft, fsm.MetadataCommand{
			Type:    fsm.CmdUpdateRepairJob,
			Payload: payload,
		})
		if err != nil {
			rs.logger.Error("schedule propsing failed", err, "chunkID", val.ChunkID)
			continue
		}
		rs.jobs <- val.JobID
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

// OnJobComplete is called by gRPC handler when a storage node reports repair outcome. Proposes CmdUpdateRepairJob with Done or Failed status. If failed and chunk still under-replicated, re-schedules.
func (rs *RepairScheduler) OnJobComplete(nodeid, jobID string, success bool, errorMsg string) error {
	if success {
		rs.mutex.Lock()
		jobsForNode, ok := rs.pendingJobs[nodeid]
		if !ok {
			rs.mutex.Unlock()
			return errors.New("invalid node")
		}

		foundAt := -1
		for idx, val := range jobsForNode {
			if val == jobID {
				foundAt = idx
				break
			}
		}
		if foundAt < 0 {
			rs.mutex.Unlock()
			return errors.New("invalid jobID")
		}
		removed := jobsForNode[:foundAt]
		removed = append(removed, jobsForNode[foundAt+1:]...)
		rs.pendingJobs[nodeid] = removed
		rs.mutex.Unlock()
	}
	return nil
}

type PlacementStrategy interface {
	SelectNodes(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
	SelectNodeReverse(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
	SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry
}

func NewRepairScheduler(raft *raft.Raft, mfsm *fsm.MetadataFSM, placement PlacementStrategy, rf int) *RepairScheduler {
	newScheduler := &RepairScheduler{
		fsm:               mfsm,
		jobs:              make(chan string, WORKER_COUNT),
		raft:              raft,
		pendingJobs:       map[string][]string{},
		placementStrategy: placement,
		logger:            logging.NewCLogger().With("component", "RepairScheduler"),
	}
	if rf <= 0 {
		rf = DEFAULT_REPLICATION_FACTOR
	}
	newScheduler.replicationFactor = min(rf, MAX_REPLICATION_FACTOR)
	return newScheduler
}
