package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
)

// MetadataFSM is the core finite-state machine that holds all metadata
// for the distributed file system. It implements the raft.FSM interface
// and is the single source of truth for file, chunk, node, and repair-job
// state. All mutations flow through Apply() via Raft log entries, ensuring
// deterministic replicated state across the cluster.
//
// Each registry has its own RWMutex to allow concurrent reads while
// serialising writes. Read methods acquire an RLock; Apply handlers
// acquire a write Lock only on the registries they mutate.
type MetadataFSM struct {
	logger            *logging.CLogger
	fiMutex           sync.RWMutex
	FileIndex         map[string]*FileRecord    // file_id → file metadata
	crMutex           sync.RWMutex
	ChunkRegistry     map[string]*ChunkRecord   // chunk_id → chunk metadata + replica list
	nrMutex           sync.RWMutex
	NodeRegistry      map[string]*NodeEntry      // node_id → storage-node info + status
	jrMutex           sync.RWMutex
	RepairJobRegistry map[string]*RepairJob      // job_id → repair job state
}

// Sentinel errors returned by registry lookups.
var (
	ErrNodeNotFound  = errors.New("node does not exist")
	ErrFileNotFound  = errors.New("file does not exist")
	ErrChunkNotFound = errors.New("chunk does not exist")
	ErrJobNotFound   = errors.New("job does not exist")
)

// NewEmptyMetadataFsm creates a MetadataFSM with empty registries.
// This is the starting state for a fresh node; Raft will populate the
// FSM either by replaying committed log entries or by restoring a snapshot.
func NewEmptyMetadataFsm(logger *logging.CLogger) *MetadataFSM {
	return &MetadataFSM{
		logger:            logger,
		FileIndex:         map[string]*FileRecord{},
		ChunkRegistry:     map[string]*ChunkRecord{},
		NodeRegistry:      map[string]*NodeEntry{},
		RepairJobRegistry: map[string]*RepairJob{},
	}
}

// Apply is called by Raft on every committed log entry. It is the ONLY
// place where FSM state is mutated. The method deserialises the raw log
// bytes into a MetadataCommand, dispatches to the appropriate handler
// based on command type, and returns the handler result.
//
// Apply MUST be deterministic — given the same sequence of commands every
// FSM replica must produce identical state. No randomness, no calls to
// time.Now(); all timestamps are carried inside the command payload.
//
// By convention the return value is nil on success or an error on failure.
func (mfsm *MetadataFSM) Apply(rlog *raft.Log) interface{} {
	var cmd MetadataCommand
	if err := json.Unmarshal(rlog.Data, &cmd); err != nil {
		mfsm.logger.Error("Apply: failed to unmarshal MetadataCommand", err)
		return err
	}

	switch cmd.Type {

	// Node commands
	case CmdRegisterNode:
		var req CommandRegisterNode
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdRegisterNode(req)

	case CmdDeregisterNode:
		var req CommandDeregisterNode
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdDeregisterNode(req)

	case CmdMarkNodeDead:
		var req CommandMarkNodeDead
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdMarkNodeDead(req)

	case CmdMarkNodeAlive:
		var req CommandMarkNodeAlive
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdMarkNodeAlive(req)

	case CmdUpdateNodeSpace:
		var req CommandUpdateNodeSpace
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdUpdateNodeSpace(req)

	// File commands
	case CmdCreateFile:
		var req CommandCreateFile
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdCreateFile(req)

	case CmdCommitFile:
		var req CommandCommitFile
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdCommitFile(req)

	case CmdDeleteFile:
		var req CommandDeleteFile
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdDeleteFile(req)

	case CmdCommitChunk:
		var req CommandCommitChunk
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdCommitChunk(req)

	case CmdEvictChunkFromNode:
		var req CommandEvictChunkFromNode
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdEvictChunkFromNode(req)

	case CmdMarkChunkLost:
		var req CommandMarkChunkLost
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdMarkChunkLost(req)

	// Repair-job commands
	case CmdCreateRepairJob:
		var req CommandCreateRepairJob
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdCreateRepairJob(req)

	case CmdUpdateRepairJob:
		var req CommandUpdateRepairJob
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdUpdateRepairJob(req)

	default:
		mfsm.logger.Error("Apply: unknown command type", nil, "type", cmd.Type)
		return errors.New("unknown command type")
	}
}

// Snapshot is called by Raft when it wants to compact the log.
// It must serialise the entire current FSM state into an FSMSnapshot.
// The implementation should take a read lock, deep-copy all four
// registries, release the lock, and then serialise the copies so that
// Apply is not blocked during the (potentially slow) serialisation.
//
// TODO: implement full snapshot serialisation.
func (mfsm *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

// Restore is called by Raft on startup (when a snapshot exists) or when
// a lagging follower needs to catch up. It must completely replace the
// current FSM state with the contents of the snapshot.
//
// TODO: implement full snapshot restoration.
func (mfsm *MetadataFSM) Restore(snapshot io.ReadCloser) error {
	return nil
}

// Command Handlers
//
// Each handler corresponds to exactly one MetadataCmdType constant.
// Handlers are the sole mutators of FSM state and are only called from
// Apply(). They must be deterministic — no time.Now(), no randomness.

// handleCmdRegisterNode adds a new storage node to the NodeRegistry or
// re-activates an existing one. If the node already exists its address,
// capacity, and status are updated (useful when a node restarts).
func (mfsm *MetadataFSM) handleCmdRegisterNode(req CommandRegisterNode) error {
	node, err := mfsm.getNodeEntryForID(req.NodeID)
	if err == nil {
		// Node already known — update it in place.
		mfsm.logger.Debug("CmdRegisterNode: node already exists... updating it")

		node.Address = req.Address
		node.Status = NodeStatusAlive
		node.FreeSpace = req.FreeSpace
		node.ChunkCount = req.ChunkCount
		node.UpdatedAt = req.CreatedAt
	} else {
		// First time we see this node — create a fresh entry.
		mfsm.logger.Debug("cmdRegisterNode: registering new node ", slog.String("id", req.NodeID))
		node = &NodeEntry{
			Address:      req.Address,
			NodeID:       req.NodeID,
			FreeSpace:    req.FreeSpace,
			RegisteredAt: req.CreatedAt,
			UpdatedAt:    req.CreatedAt,
			Status:       NodeStatusAlive,
			ChunkCount:   req.ChunkCount,
		}
	}
	return upsert(&mfsm.nrMutex, mfsm.NodeRegistry, node.NodeID, node)
}

// handleCmdDeregisterNode marks a storage node as "draining".
// This is triggered by a graceful shutdown of the storage node.
// The NodeWatcher / RepairScheduler will subsequently handle
// migrating chunks off this node.
func (mfsm *MetadataFSM) handleCmdDeregisterNode(req CommandDeregisterNode) error {
	node, err := mfsm.getNodeEntryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("cmdDeregisterNode failed: "), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.Status = NodeStatusDraining
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	return nil
}

// handleCmdMarkNodeDead transitions a node to the "dead" state.
// Triggered by the NodeWatcher when heartbeat timeout is exceeded.
// Once dead the node is excluded from placement eligibility and
// its chunks become candidates for re-replication.
func (mfsm *MetadataFSM) handleCmdMarkNodeDead(req CommandMarkNodeDead) error {
	node, err := mfsm.getNodeEntryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("cmdMarkNodeDead failed: "), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.Status = NodeStatusDead
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	return nil
}

// handleCmdMarkNodeAlive transitions a node back to "alive".
// Used during node re-registration after temporary failure.
func (mfsm *MetadataFSM) handleCmdMarkNodeAlive(req CommandMarkNodeAlive) error {
	node, err := mfsm.getNodeEntryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("cmdMarkNodeAlive failed: "), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.Status = NodeStatusAlive
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	return nil
}

// handleCmdUpdateNodeSpace updates the free-space and chunk-count
// metrics for a storage node. This is proposed periodically from
// heartbeat data (not every heartbeat — roughly every Nth to avoid
// log spam).
func (mfsm *MetadataFSM) handleCmdUpdateNodeSpace(req CommandUpdateNodeSpace) error {
	node, err := mfsm.getNodeEntryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("cmdUpdateNodeSpace failed: "), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.FreeSpace = req.FreeSpace
	node.ChunkCount = req.ChunkCount
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	return nil
}

// File command handlers

// handleCmdCreateFile records a new file in the FileIndex and creates
// stub ChunkRecords (status: allocated) in the ChunkRegistry.
//
// The method performs a two-pass insert on the ChunkRegistry:
//   1. Validate that none of the chunk IDs collide with existing records.
//   2. Insert all chunk records atomically under a single lock hold.
//
// The file starts in FileStatusCreating; it transitions to
// FileStatusComplete only after a subsequent CmdCommitFile.
func (mfsm *MetadataFSM) handleCmdCreateFile(req CommandCreateFile) error {
	// Reject duplicate file IDs.
	_, err := mfsm.getFileEntryForID(req.FileID)
	if err == nil {
		return errors.New("CmdCreateFile: fileID already exists in our systems, cannot override")
	}

	file := &FileRecord{
		FileID:    req.FileID,
		Filename:  req.FileName,
		FileSize:  req.FileSize,
		ChunkIDs:  req.ChunkIDs,
		Status:    FileStatusCreating,
		CreatedAt: req.CreatedAt,
	}

	// Build chunk stubs for every chunk belonging to this file.
	chunkRecords := make([]*ChunkRecord, 0, len(req.ChunkIDs))
	for i, cid := range req.ChunkIDs {
		chunkRecords = append(chunkRecords, &ChunkRecord{
			ChunkID:    cid,
			FileID:     req.FileID,
			ChunkIndex: i,
			Status:     ChunkStatusRequestAllocation,
		})
	}

	// Assumption: unique fileID ⇒ unique chunkIDs, so collisions should
	// practically never happen; we guard against it anyway.

	mfsm.crMutex.Lock()
	// Pass 1 — verify no collisions.
	for _, val := range chunkRecords {
		if _, ok := mfsm.ChunkRegistry[val.ChunkID]; ok {
			mfsm.crMutex.Unlock()
			return errors.New("CmdCreateFile: chunkID already exists in our systems, unsupported")
		}
	}
	// Pass 2 — insert all chunks.
	for _, val := range chunkRecords {
		mfsm.ChunkRegistry[val.ChunkID] = val
	}
	mfsm.crMutex.Unlock()

	mfsm.fiMutex.Lock()
	defer mfsm.fiMutex.Unlock()
	mfsm.FileIndex[req.FileID] = file

	return nil
}

// handleCmdCommitFile transitions a file from "creating" to "complete".
// Before committing it validates:
//   - File size matches the original declaration.
//   - Checksum matches the original declaration.
//   - Every chunk belonging to this file is in ChunkStatusComplete.
//
// If any validation fails the file status is left unchanged and an
// error is returned.
func (mfsm *MetadataFSM) handleCmdCommitFile(req CommandCommitFile) error {
	file, err := mfsm.getFileEntryForID(req.FileID)
	if err != nil {
		return errors.Join(errors.New("cmdCommitFile: "), err)
	}

	if file.FileSize != req.FileSize {
		return errors.New("cmdCommitFile: filesize mismatch")
	}
	if !bytes.Equal(file.CheckSum, req.Checksum) {
		return errors.New("cmdCommitFile: checksum mismatch")
	}

	// Validate that every chunk has been fully replicated and committed.
	mfsm.crMutex.Lock()
	for _, cid := range file.ChunkIDs {
		ck, ok := mfsm.ChunkRegistry[cid]
		if !ok {
			mfsm.crMutex.Unlock()
			err := errors.New("cmdCommitFile: chunk validation failed : chunk not found")
			mfsm.logger.Error("cmdCommitFile failed", err, "chunk_id", cid)
			return err
		}

		if ck.Status != ChunkStatusComplete {
			mfsm.crMutex.Unlock()
			err := errors.New("cmdCommitFile: chunk validation failed : chunk not commited")
			mfsm.logger.Error("cmdCommitFile failed", err, "chunk_id", cid)
			return err
		}
	}
	mfsm.crMutex.Unlock()

	mfsm.fiMutex.Lock()
	defer mfsm.fiMutex.Unlock()
	file.Status = FileStatusComplete
	mfsm.logger.Debug("Commited file", "id", file.FileID)
	return nil
}

// handleCmdDeleteFile marks a file as deleted. The actual eviction of
// chunk replicas from storage nodes is handled asynchronously by the
// RepairScheduler / eviction queue — this handler only updates the
// canonical metadata state.
func (mfsm *MetadataFSM) handleCmdDeleteFile(req CommandDeleteFile) error {
	file, err := mfsm.getFileEntryForID(req.FileID)
	if err != nil {
		return errors.Join(errors.New("cmdDeleteFile: "), err)
	}

	mfsm.fiMutex.Lock()
	defer mfsm.fiMutex.Unlock()

	file.Status = FileStatusDeleted
	return nil
}

// handleCmdCommitChunk finalises a chunk after replication is confirmed.
// It records the verified checksum, sets the confirmed replica list,
// and transitions the chunk to ChunkStatusComplete.
//
// The handler guards against double-commits: a chunk that already has a
// checksum or is already Complete is rejected.
func (mfsm *MetadataFSM) handleCmdCommitChunk(req CommandCommitChunk) error {
	chunk, err := mfsm.getChunkEntryForID(req.ChunkID)
	if err != nil {
		return errors.Join(errors.New("cmdCommitChunk: "), err)
	}
	if len(chunk.Checksum) == 0 && chunk.Status != ChunkStatusComplete {
		mfsm.crMutex.Lock()
		chunk.Status = ChunkStatusComplete
		chunk.Checksum = req.Checksum
		chunk.Replicas = req.NodeIDs // confirmed by storage node
		mfsm.crMutex.Unlock()
		return nil
	}
	return errors.New("cmdCommitChunk: chunk validation failed")
}

// handleCmdEvictChunkFromNode removes a single node from a chunk's
// replica list. This is used during reconciliation (stale replica),
// corruption reporting, or node death clean-up.
func (mfsm *MetadataFSM) handleCmdEvictChunkFromNode(req CommandEvictChunkFromNode) error {
	chunk, err := mfsm.getChunkEntryForID(req.ChunkID)
	if err != nil {
		return errors.Join(errors.New("cmdEvictChunk: "), err)
	}
	mfsm.crMutex.Lock()
	defer mfsm.crMutex.Unlock()

	// Locate the node in the replica list.
	getIndex := func() int {
		for i, val := range chunk.Replicas {
			if val == req.NodeID {
				return i
			}
		}
		return -1
	}

	idx := getIndex()
	if idx == -1 {
		return errors.New("cmdEvictChunk: chunk not present on node")
	}
	chunk.Replicas = append(chunk.Replicas[:idx], chunk.Replicas[idx+1:]...)
	return nil
}

// handleCmdMarkChunkLost marks a chunk as permanently lost.
// This is the terminal state when repair has been exhausted and no
// live replicas remain.
func (mfsm *MetadataFSM) handleCmdMarkChunkLost(req CommandMarkChunkLost) error {
	chunk, err := mfsm.getChunkEntryForID(req.ChunkID)
	if err != nil {
		return errors.Join(errors.New("cmdMarkChunkLost: "), err)
	}
	mfsm.crMutex.Lock()
	chunk.Status = ChunkStatusLost
	mfsm.crMutex.Unlock()
	return nil
}

// Repair-job command handlers

// handleCmdCreateRepairJob creates a new repair job in the
// RepairJobRegistry. Duplicate job IDs are rejected.
//
// NOTE: source/target node IDs and chunk ID are currently left empty;
// they will be populated by the RepairScheduler in a follow-up
// CmdUpdateRepairJob once placement has been decided.
func (mfsm *MetadataFSM) handleCmdCreateRepairJob(req CommandCreateRepairJob) error {
	_, err := mfsm.getJobEntryForID(req.JobID)
	if err == nil {
		return errors.New("cmdCreateRepairJob: job already exists")
	}

	newJob := &RepairJob{
		JobID:        req.JobID,
		ChunkID:      "",
		SourceNodeID: "",
		TargetNodeID: "",
		Status:       RepairStatusPending,
		Attempts:     0,
		CreatedAt:    req.CreatedAt,
		UpdatedAt:    req.CreatedAt,
	}
	return upsert(&mfsm.jrMutex, mfsm.RepairJobRegistry, req.JobID, newJob)
}

// handleCmdUpdateRepairJob mutates an existing repair job's status,
// attempt counter, and optional error message. Used by the gRPC handler
// when a storage node reports repair completion or failure.
func (mfsm *MetadataFSM) handleCmdUpdateRepairJob(req CommandUpdateRepairJob) error {
	job, err := mfsm.getJobEntryForID(req.JobID)
	if err != nil {
		return errors.Join(errors.New("cmdUpdateRepairJob: "), err)
	}
	mfsm.jrMutex.Lock()
	defer mfsm.jrMutex.Unlock()
	job.Status = req.Status
	job.UpdatedAt = req.UpdatedAt
	job.Error = req.Error
	job.Attempts = req.Attempts
	return nil
}

// Internal helpers

// upsert inserts or overwrites an entry in the given registry map.
// If mu is non-nil the lock is acquired before the write and released
// on return. Passing mu == nil is allowed for callers that manage
// locking externally.
func upsert[T any](mu sync.Locker, registry map[string]T, key string, entry T) error {
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	if registry == nil {
		return errors.New("upsert: registry is nil")
	}
	registry[key] = entry
	return nil
}

// getNodeEntryForID performs a read-locked lookup of a NodeEntry.
// Returns ErrNodeNotFound if the node is not in the registry.
func (mfsm *MetadataFSM) getNodeEntryForID(nodeID string) (*NodeEntry, error) {
	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()
	node, ok := mfsm.NodeRegistry[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return node, nil
}

// getFileEntryForID performs a read-locked lookup of a FileRecord.
// Returns ErrFileNotFound if the file is not in the index.
func (mfsm *MetadataFSM) getFileEntryForID(fileID string) (*FileRecord, error) {
	mfsm.fiMutex.RLock()
	defer mfsm.fiMutex.RUnlock()
	file, ok := mfsm.FileIndex[fileID]
	if !ok {
		return nil, ErrFileNotFound
	}
	return file, nil
}

// getChunkEntryForID performs a read-locked lookup of a ChunkRecord.
// Returns ErrChunkNotFound if the chunk is not in the registry.
func (mfsm *MetadataFSM) getChunkEntryForID(chunkID string) (*ChunkRecord, error) {
	mfsm.crMutex.RLock()
	defer mfsm.crMutex.RUnlock()
	chunk, ok := mfsm.ChunkRegistry[chunkID]
	if !ok {
		return nil, ErrChunkNotFound
	}
	return chunk, nil
}

// getJobEntryForID performs a read-locked lookup of a RepairJob.
// Returns ErrJobNotFound if the job is not in the registry.
func (mfsm *MetadataFSM) getJobEntryForID(jobID string) (*RepairJob, error) {
	mfsm.jrMutex.RLock()
	defer mfsm.jrMutex.RUnlock()
	job, ok := mfsm.RepairJobRegistry[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}
