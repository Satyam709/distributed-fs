package scheduler

import (
	"context"
	"encoding/json"
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
	job               chan *fsm.RepairJob
	replicationFactor int
	placementStrategy PlacementStrategy
	logger            *logging.CLogger
}

// Start repair worker goroutines consuming an internal job channel. Also start a recovery goroutine that runs once on startup to recover stuck in-progress jobs.
func (rs *RepairScheduler) Start(ctx context.Context) {
	go func() {
		workers := make(chan struct{}, WORKER_COUNT)
		for {
			select {
			case <-ctx.Done():
				rs.logger.Info("stoping: ctx cancelled")
				return
			case cj := <-rs.job:
				// assign job
				workers <- struct{}{}
				rs.executeJob(ctx, cj)
				// release worker
				<-workers
			}
		}
	}()

	// run on startup
	go rs.RecoverStuckJobs()
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
	asyncSendToChannel := func(id string) {
		rjob, err := rs.fsm.GetRepairJob(id)
		if err != nil {
			return
		}
		rs.job <- rjob
	}
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
			go asyncSendToChannel(jid)
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
			go asyncSendToChannel(jid)
		}
	}
}

// executeJob delivers the repair instruction to the source node. Repair instructions are piggybacked on heartbeat responses — so this method adds the job to a pending map keyed by source node ID. When that node next heartbeats, the handler includes the pending job in the response.
func (rs *RepairScheduler) executeJob(ctx context.Context, job *fsm.RepairJob) {
}

// RecoverStuckJobs scans all jobs with status InProgress via fsm.GetJobsByStatus(InProgress). For each, checks if source node is still alive. If not, resets job to pending and re-enqueues with a new source node.
func (rs *RepairScheduler) RecoverStuckJobs() {
	rs.logger.Info("recovering the pending jobs...")
	stuckJobs, err := rs.fsm.GetJobsByStatus(fsm.RepairStatusInProgress)
	if err != nil {
		rs.logger.Error("failed to get stuckjobs", err)
	}
	asyncSendToChannel := func(id string) {
		rjob, err := rs.fsm.GetRepairJob(id)
		if err != nil {
			return
		}
		rs.job <- rjob
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
			go asyncSendToChannel(val.JobID)
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
		go asyncSendToChannel(val.JobID)
	}
}

// OnJobComplete is called by gRPC handler when a storage node reports repair outcome. Proposes CmdUpdateRepairJob with Done or Failed status. If failed and chunk still under-replicated, re-schedules.
func (rs *RepairScheduler) OnJobComplete(jobID string, success bool, errorMsg string) {
}

type PlacementStrategy interface {
	SelectNodes(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
	SelectNodeReverse(chunkID string, count int, exclude ...fsm.NodeEntry) ([]fsm.NodeEntry, error)
	SelectPrimary(nodes []fsm.NodeEntry) fsm.NodeEntry
}

func NewRepairScheduler(raft *raft.Raft, mfsm *fsm.MetadataFSM, placement PlacementStrategy, rf int) *RepairScheduler {
	newScheduler := &RepairScheduler{
		fsm:               mfsm,
		job:               make(chan *fsm.RepairJob),
		raft:              raft,
		placementStrategy: placement,
		logger:            logging.NewCLogger().With("component", "RepairScheduler"),
	}
	if rf <= 0 {
		rf = DEFAULT_REPLICATION_FACTOR
	}
	newScheduler.replicationFactor = min(rf, MAX_REPLICATION_FACTOR)
	return newScheduler
}
