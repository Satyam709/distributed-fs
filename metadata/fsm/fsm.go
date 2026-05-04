package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/internal/utils"
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
	fileIndex         map[string]*FileRecord // file_id → file metadata
	crMutex           sync.RWMutex
	chunkRegistry     map[string]*ChunkRecord // chunk_id → chunk metadata + replica list
	nrMutex           sync.RWMutex
	nodeRegistry      map[string]*NodeEntry // node_id → storage-node info + status
	jrMutex           sync.RWMutex
	repairJobRegistry map[string]*RepairJob // job_id → repair job state
	mnMutex           sync.RWMutex
	mdNodeRegistry    map[string]*MetadataNodeEntry // raft_addr → metadata node info
}

var _ raft.FSMSnapshot = &MetadataFSMSnapshot{}

type MetadataFSMSnapshot struct {
	FileIndex         map[string]FileRecord        `json:"file_index"`
	ChunkRegistry     map[string]ChunkRecord       `json:"chunk_registry"`
	NodeRegistry      map[string]NodeEntry         `json:"node_registry"`
	RepairJobRegistry map[string]RepairJob         `json:"repairjob_registry"`
	MDNodeRegistry    map[string]MetadataNodeEntry `json:"md_node_registry"`
}

// RestoreFSM constructs a new MetadataFSM from the snapshot's deep-copied,
// value-typed registries. Pointer maps are rebuilt so that the returned FSM
// is ready for use by Apply() and the read methods.
func (fsms *MetadataFSMSnapshot) RestoreFSM(logger *logging.CLogger) *MetadataFSM {
	newFSM := &MetadataFSM{
		logger:            logger,
		fileIndex:         make(map[string]*FileRecord, len(fsms.FileIndex)),
		chunkRegistry:     make(map[string]*ChunkRecord, len(fsms.ChunkRegistry)),
		nodeRegistry:      make(map[string]*NodeEntry, len(fsms.NodeRegistry)),
		repairJobRegistry: make(map[string]*RepairJob, len(fsms.RepairJobRegistry)),
		mdNodeRegistry:    make(map[string]*MetadataNodeEntry, len(fsms.MDNodeRegistry)),
	}

	for k, v := range fsms.FileIndex {
		copy := v // copy the value so each entry gets its own allocation
		newFSM.fileIndex[k] = &copy
	}
	for k, v := range fsms.ChunkRegistry {
		copy := v
		newFSM.chunkRegistry[k] = &copy
	}
	for k, v := range fsms.NodeRegistry {
		copy := v
		newFSM.nodeRegistry[k] = &copy
	}
	for k, v := range fsms.RepairJobRegistry {
		copy := v
		newFSM.repairJobRegistry[k] = &copy
	}
	for k, v := range fsms.MDNodeRegistry {
		copy := v
		newFSM.mdNodeRegistry[k] = &copy
	}

	return newFSM
}

// Persist serialises the entire snapshot to the given SnapshotSink.
// On success it closes the sink; on any error it cancels the sink to
// signal Raft that the snapshot should be discarded.
func (fsms *MetadataFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// Ensure the sink is properly finalised regardless of outcome.
	var err error
	defer func() {
		if err != nil {
			_ = sink.Cancel()
		}
	}()

	data, err := json.Marshal(fsms)
	if err != nil {
		return err
	}

	n, err := io.Copy(sink, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if n != int64(len(data)) {
		err = errors.New("Persist: short write")
		return err
	}

	// Close the sink to signal success to Raft.
	err = sink.Close()
	return err
}

func (fsms *MetadataFSMSnapshot) Release() {
	// pass
}

// Sentinel errors returned by registry lookups.
var (
	ErrNodeNotFound         = errors.New("node does not exist")
	ErrFileNotFound         = errors.New("file does not exist")
	ErrChunkNotFound        = errors.New("chunk does not exist")
	ErrJobNotFound          = errors.New("job does not exist")
	ErrMetadataNodeNotFound = errors.New("metadata node does not exist")
)

// NewEmptyMetadataFsm creates a MetadataFSM with empty registries.
// This is the starting state for a fresh node; Raft will populate the
// FSM either by replaying committed log entries or by restoring a snapshot.
func NewEmptyMetadataFsm(logger *logging.CLogger) *MetadataFSM {
	return &MetadataFSM{
		logger:            logger,
		fileIndex:         map[string]*FileRecord{},
		chunkRegistry:     map[string]*ChunkRecord{},
		nodeRegistry:      map[string]*NodeEntry{},
		repairJobRegistry: map[string]*RepairJob{},
		mdNodeRegistry:    map[string]*MetadataNodeEntry{},
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

	case CmdAddChunkReplica:
		var req CommandAddChunkReplica
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdAddChunkReplica(req)

	case CmdRegisterMetadataNode:
		var req CommandRegisterMetadataNode
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdRegisterMetadataNode(req)

	case CmdDeregisterMetadataNode:
		var req CommandDeregisterMetadataNode
		if err := json.Unmarshal(cmd.Payload, &req); err != nil {
			return err
		}
		return mfsm.handleCmdDeregisterMetadataNode(req)

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
func (mfsm *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	mfsm.crMutex.RLock()
	mfsm.fiMutex.RLock()
	mfsm.jrMutex.RLock()
	mfsm.nrMutex.RLock()
	mfsm.mnMutex.RLock()

	fiCopy := utils.DeepCopy(mfsm.fileIndex)
	crCopy := utils.DeepCopy(mfsm.chunkRegistry)
	nrCopy := utils.DeepCopy(mfsm.nodeRegistry)
	jrCopy := utils.DeepCopy(mfsm.repairJobRegistry)
	mnCopy := utils.DeepCopy(mfsm.mdNodeRegistry)

	mfsm.crMutex.RUnlock()
	mfsm.fiMutex.RUnlock()
	mfsm.jrMutex.RUnlock()
	mfsm.nrMutex.RUnlock()
	mfsm.mnMutex.RUnlock()

	snap := &MetadataFSMSnapshot{
		FileIndex:         fiCopy,
		ChunkRegistry:     crCopy,
		NodeRegistry:      nrCopy,
		RepairJobRegistry: jrCopy,
		MDNodeRegistry:    mnCopy,
	}

	return snap, nil
}

// Restore is called by Raft on startup (when a snapshot exists) or when
// a lagging follower needs to catch up. It must completely replace the
// current FSM state with the contents of the snapshot.
func (mfsm *MetadataFSM) Restore(snapshot io.ReadCloser) error {
	defer func() {
		if err := snapshot.Close(); err != nil {
			mfsm.logger.Error("Restore: snapshot closure failed", err)
		}
	}()

	data, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}

	snap := &MetadataFSMSnapshot{}
	if err = json.Unmarshal(data, snap); err != nil {
		return err
	}

	// Rebuild pointer-typed registries from the snapshot.
	restored := snap.RestoreFSM(mfsm.logger)

	// Swap all registries under write locks.
	mfsm.fiMutex.Lock()
	mfsm.fileIndex = restored.fileIndex
	mfsm.fiMutex.Unlock()

	mfsm.crMutex.Lock()
	mfsm.chunkRegistry = restored.chunkRegistry
	mfsm.crMutex.Unlock()

	mfsm.nrMutex.Lock()
	mfsm.nodeRegistry = restored.nodeRegistry
	mfsm.nrMutex.Unlock()

	mfsm.jrMutex.Lock()
	mfsm.repairJobRegistry = restored.repairJobRegistry
	mfsm.jrMutex.Unlock()

	mfsm.mnMutex.Lock()
	mfsm.mdNodeRegistry = restored.mdNodeRegistry
	mfsm.mnMutex.Unlock()

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
	node, err := mfsm.GetNode(req.NodeID)
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
			LastSeen:     req.CreatedAt, // prevent watcher from marking as dead before first heartbeat
			Status:       NodeStatusAlive,
			ChunkCount:   req.ChunkCount,
		}
	}
	return upsert(&mfsm.nrMutex, mfsm.nodeRegistry, node.NodeID, node)
}

// handleCmdDeregisterNode marks a storage node as "draining".
// This is triggered by a graceful shutdown of the storage node.
// The NodeWatcher / RepairScheduler will subsequently handle
// migrating chunks off this node.
func (mfsm *MetadataFSM) handleCmdDeregisterNode(req CommandDeregisterNode) error {
	node, err := mfsm.GetNode(req.NodeID)
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
	node, err := mfsm.GetNode(req.NodeID)
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
	node, err := mfsm.GetNode(req.NodeID)
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
	node, err := mfsm.GetNode(req.NodeID)
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
//  1. Validate that none of the chunk IDs collide with existing records.
//  2. Insert all chunk records atomically under a single lock hold.
//
// The file starts in FileStatusCreating; it transitions to
// FileStatusComplete only after a subsequent CmdCommitFile.
func (mfsm *MetadataFSM) handleCmdCreateFile(req CommandCreateFile) error {
	// Reject duplicate file IDs.
	_, err := mfsm.GetFile(req.FileID)
	if err == nil {
		return errors.New("CmdCreateFile: fileID already exists in our systems, cannot override")
	}

	file := &FileRecord{
		FileID:    req.FileID,
		Filename:  req.FileName,
		FileSize:  req.FileSize,
		ChunkSize: req.ChunkSize,
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
			Status:     ChunkStatusAllocated,
		})
	}

	// Assumption: unique fileID ⇒ unique chunkIDs, so collisions should
	// practically never happen; we guard against it anyway.

	mfsm.crMutex.Lock()
	// Pass 1 — verify no collisions.
	for _, val := range chunkRecords {
		if _, ok := mfsm.chunkRegistry[val.ChunkID]; ok {
			mfsm.crMutex.Unlock()
			return errors.New("CmdCreateFile: chunkID already exists in our systems, unsupported")
		}
	}
	// Pass 2 — insert all chunks.
	for _, val := range chunkRecords {
		mfsm.chunkRegistry[val.ChunkID] = val
	}
	mfsm.crMutex.Unlock()

	mfsm.fiMutex.Lock()
	defer mfsm.fiMutex.Unlock()
	mfsm.fileIndex[req.FileID] = file

	return nil
}

// handleCmdCommitFile transitions a file from "creating" to "complete".
// Before committing it validates:
//   - File size matches the original declaration.
//   - Every chunk belonging to this file is in ChunkStatusComplete.
//
// The whole-file checksum is stored at commit time — it is NOT known at
// CreateFile time (the file hasn't been uploaded yet), so CommitFile is
// the authoritative source for the file-level checksum.
//
// If any validation fails the file status is left unchanged and an
// error is returned.
func (mfsm *MetadataFSM) handleCmdCommitFile(req CommandCommitFile) error {
	file, err := mfsm.GetFile(req.FileID)
	if err != nil {
		return errors.Join(errors.New("cmdCommitFile: "), err)
	}

	if file.FileSize != req.FileSize {
		return errors.New("cmdCommitFile: filesize mismatch")
	}

	// Validate that every chunk has been fully replicated and committed.
	mfsm.crMutex.Lock()
	for _, cid := range file.ChunkIDs {
		ck, ok := mfsm.chunkRegistry[cid]
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
	file.CheckSum = req.Checksum
	file.Status = FileStatusComplete
	mfsm.logger.Debug("Commited file", "id", file.FileID)
	return nil
}

// handleCmdDeleteFile marks a file as deleted. The actual eviction of
// chunk replicas from storage nodes is handled asynchronously by the
// RepairScheduler / eviction queue — this handler only updates the
// canonical metadata state.
func (mfsm *MetadataFSM) handleCmdDeleteFile(req CommandDeleteFile) error {
	file, err := mfsm.GetFile(req.FileID)
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
	chunk, err := mfsm.GetChunk(req.ChunkID)
	if err != nil {
		return errors.Join(errors.New("cmdCommitChunk: "), err)
	}
	if len(chunk.Checksum) == 0 && chunk.Status != ChunkStatusComplete {
		mfsm.crMutex.Lock()
		chunk.Status = ChunkStatusComplete
		chunk.Checksum = req.Checksum
		chunk.Replicas = mfsm.normalizeReplicaNodeIDs(req.NodeIDs)
		mfsm.crMutex.Unlock()
		return nil
	}
	return errors.New("cmdCommitChunk: chunk validation failed")
}

// normalizeReplicaNodeIDs converts each entry in rawIDs to a valid node ID.
// If an entry is already a node ID (found in nodeRegistry), it is kept as-is.
// If an entry is an address (not found as a key), the nodeRegistry is scanned
// for a node whose Address matches — the node's NodeID is used instead.
// Entries that match neither a node ID nor an address are dropped.
func (mfsm *MetadataFSM) normalizeReplicaNodeIDs(rawIDs []string) []string {
	normalized := make([]string, 0, len(rawIDs))
	seen := make(map[string]struct{}, len(rawIDs))

	for _, entry := range rawIDs {
		if _, already := seen[entry]; already {
			continue
		}

		if _, err := mfsm.GetNode(entry); err == nil {
			normalized = append(normalized, entry)
			seen[entry] = struct{}{}
			continue
		}

		resolved := ""
		mfsm.nrMutex.RLock()
		for _, node := range mfsm.nodeRegistry {
			if node.Address == entry {
				resolved = node.NodeID
				break
			}
		}
		mfsm.nrMutex.RUnlock()

		if resolved != "" {
			if _, already := seen[resolved]; already {
				continue
			}
			normalized = append(normalized, resolved)
			seen[resolved] = struct{}{}
		}
	}

	return normalized
}

// handleCmdEvictChunkFromNode removes a single node from a chunk's
// replica list. This is used during reconciliation (stale replica),
// corruption reporting, or node death clean-up.
func (mfsm *MetadataFSM) handleCmdEvictChunkFromNode(req CommandEvictChunkFromNode) error {
	chunk, err := mfsm.GetChunk(req.ChunkID)
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
	chunk, err := mfsm.GetChunk(req.ChunkID)
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
	mfsm.jrMutex.RLock()
	_, exists := mfsm.repairJobRegistry[req.JobID]
	mfsm.jrMutex.RUnlock()
	if exists {
		return errors.New("cmdCreateRepairJob: job already exists")
	}

	newJob := &RepairJob{
		JobID:        req.JobID,
		ChunkID:      req.ChunkID,
		DeleteSource: req.DeleteSource,
		SourceNodeID: req.SourceNode,
		TargetNodeID: req.TargetNode,
		Status:       RepairStatusPending,
		Attempts:     0,
		CreatedAt:    req.CreatedAt,
		UpdatedAt:    req.CreatedAt,
	}
	return upsert(&mfsm.jrMutex, mfsm.repairJobRegistry, req.JobID, newJob)
}

// handleCmdUpdateRepairJob mutates an existing repair job's status,
// attempt counter, and optional error message. Used by the gRPC handler
// when a storage node reports repair completion or failure.
func (mfsm *MetadataFSM) handleCmdUpdateRepairJob(req CommandUpdateRepairJob) error {
	mfsm.jrMutex.Lock()
	defer mfsm.jrMutex.Unlock()
	job, ok := mfsm.repairJobRegistry[req.JobID]
	if !ok {
		return errors.Join(errors.New("cmdUpdateRepairJob: "), ErrJobNotFound)
	}
	job.Status = req.Status
	job.UpdatedAt = req.UpdatedAt
	job.Error = req.Error
	job.Attempts = req.Attempts
	if req.SourceNode != "" {
		job.SourceNodeID = req.SourceNode
	}
	return nil
}

// handleCmdAddChunkReplica adds a single node to a chunk's replica list.
// This is used after a successful repair to register the new replica.
// The operation is idempotent — if the node is already present, it's a no-op.
func (mfsm *MetadataFSM) handleCmdAddChunkReplica(req CommandAddChunkReplica) error {
	chunk, err := mfsm.GetChunk(req.ChunkID)
	if err != nil {
		return errors.Join(errors.New("cmdAddChunkReplica: "), err)
	}
	mfsm.crMutex.Lock()
	defer mfsm.crMutex.Unlock()
	for _, nid := range chunk.Replicas {
		if nid == req.NodeID {
			return nil // already present, idempotent
		}
	}
	chunk.Replicas = append(chunk.Replicas, req.NodeID)
	return nil
}

func (mfsm *MetadataFSM) handleCmdRegisterMetadataNode(req CommandRegisterMetadataNode) error {
	entry := &MetadataNodeEntry{
		NodeID:   req.NodeID,
		RaftAddr: req.RaftAddr,
		GrpcAddr: req.GrpcAddr,
	}
	return upsert(&mfsm.mnMutex, mfsm.mdNodeRegistry, req.RaftAddr, entry)
}

func (mfsm *MetadataFSM) handleCmdDeregisterMetadataNode(req CommandDeregisterMetadataNode) error {
	mfsm.mnMutex.Lock()
	defer mfsm.mnMutex.Unlock()
	delete(mfsm.mdNodeRegistry, req.RaftAddr)
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

// Read Methods — Public API
//
// These are called by the gRPC handler to serve reads. They do NOT go
// through Raft — they are direct in-memory reads protected by RLocks.

// File reads

// GetFile performs a read-locked lookup of a FileRecord by file ID.
// Returns ErrFileNotFound if the file is not in the index.
func (mfsm *MetadataFSM) GetFile(fileID string) (*FileRecord, error) {
	mfsm.fiMutex.RLock()
	defer mfsm.fiMutex.RUnlock()
	file, ok := mfsm.fileIndex[fileID]
	if !ok {
		return nil, ErrFileNotFound
	}
	return file, nil
}

// GetFileByName performs a linear scan of the FileIndex to find a file
// by its human-readable filename. Returns ErrFileNotFound if no match
// exists. If multiple files share the same name only the first match
// (non-deterministic map order) is returned.
func (mfsm *MetadataFSM) GetFileByName(filename string) (*FileRecord, error) {
	mfsm.fiMutex.RLock()
	defer mfsm.fiMutex.RUnlock()
	for _, file := range mfsm.fileIndex {
		if file.Filename == filename {
			return file, nil
		}
	}
	return nil, ErrFileNotFound
}

// ListFiles returns all files whose filename starts with the given prefix.
// An empty prefix matches every file. Results are sorted by filename for
// deterministic output.
func (mfsm *MetadataFSM) ListFiles(prefix string) ([]*FileRecord, error) {
	mfsm.fiMutex.RLock()
	defer mfsm.fiMutex.RUnlock()
	result := make([]*FileRecord, 0)
	for _, file := range mfsm.fileIndex {
		if strings.HasPrefix(file.Filename, prefix) {
			result = append(result, file)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Filename < result[j].Filename
	})
	return result, nil
}

// GetFileChunks returns all ChunkRecords belonging to a file, ordered
// by ChunkIndex. Returns ErrFileNotFound if the file does not exist.
func (mfsm *MetadataFSM) GetFileChunks(fileID string) ([]*ChunkRecord, error) {
	file, err := mfsm.GetFile(fileID)
	if err != nil {
		return nil, err
	}

	mfsm.crMutex.RLock()
	defer mfsm.crMutex.RUnlock()

	chunks := make([]*ChunkRecord, 0, len(file.ChunkIDs))
	for _, cid := range file.ChunkIDs {
		ck, ok := mfsm.chunkRegistry[cid]
		if !ok {
			return nil, ErrChunkNotFound
		}
		chunks = append(chunks, ck)
	}
	return chunks, nil
}

// Chunk reads

// GetChunk performs a read-locked lookup of a ChunkRecord by chunk ID.
// Returns ErrChunkNotFound if the chunk is not in the registry.
func (mfsm *MetadataFSM) GetChunk(chunkID string) (*ChunkRecord, error) {
	mfsm.crMutex.RLock()
	defer mfsm.crMutex.RUnlock()
	chunk, ok := mfsm.chunkRegistry[chunkID]
	if !ok {
		return nil, ErrChunkNotFound
	}
	return chunk, nil
}

// GetChunkLocations returns the NodeEntry for each live replica of a
// chunk, filtering out dead nodes. Returns ErrChunkNotFound if the
// chunk does not exist.
func (mfsm *MetadataFSM) GetChunkLocations(chunkID string) ([]NodeEntry, error) {
	chunk, err := mfsm.GetChunk(chunkID)
	if err != nil {
		return nil, err
	}

	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()

	nodes := make([]NodeEntry, 0, len(chunk.Replicas))
	for _, nodeID := range chunk.Replicas {
		node, ok := mfsm.nodeRegistry[nodeID]
		if !ok {
			continue // node removed from registry
		}
		if node.Status == NodeStatusDead {
			continue
		}
		nodes = append(nodes, *node)
	}
	return nodes, nil
}

// GetChunksByNode scans the ChunkRegistry and returns the IDs of all
// chunks that have the given node in their replica list.
func (mfsm *MetadataFSM) GetChunksByNode(nodeID string) ([]string, error) {
	mfsm.crMutex.RLock()
	defer mfsm.crMutex.RUnlock()

	var chunkIDs []string
	for _, chunk := range mfsm.chunkRegistry {
		for _, nid := range chunk.Replicas {
			if nid == nodeID {
				chunkIDs = append(chunkIDs, chunk.ChunkID)
				break
			}
		}
	}
	return chunkIDs, nil
}

// Node reads

// GetNode performs a read-locked lookup of a NodeEntry by node ID.
// Returns ErrNodeNotFound if the node is not in the registry.
func (mfsm *MetadataFSM) GetNode(nodeID string) (*NodeEntry, error) {
	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()
	node, ok := mfsm.nodeRegistry[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return node, nil
}

// GetLiveNodes returns a copy of every NodeEntry with status "alive".
func (mfsm *MetadataFSM) GetLiveNodes() ([]NodeEntry, error) {
	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()

	nodes := make([]NodeEntry, 0)
	for _, node := range mfsm.nodeRegistry {
		if node.Status == NodeStatusAlive {
			nodes = append(nodes, *node)
		}
	}
	return nodes, nil
}

// GetNodeCount returns the total number of nodes in the registry
// regardless of status.
func (mfsm *MetadataFSM) GetNodeCount() int {
	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()
	return len(mfsm.nodeRegistry)
}

// GetMetadataNodeByRaftAddr looks up the grpc address for a metadata node
// given its raft address. Used by leaderRedirect() to build client-usable
// redirect responses.
func (mfsm *MetadataFSM) GetMetadataNodeByRaftAddr(raftAddr string) (MetadataNodeEntry, error) {
	mfsm.mnMutex.RLock()
	defer mfsm.mnMutex.RUnlock()
	entry, ok := mfsm.mdNodeRegistry[raftAddr]
	if !ok {
		return MetadataNodeEntry{}, ErrMetadataNodeNotFound
	}
	return *entry, nil
}

// Repair reads

// GetRepairJob performs a read-locked lookup of a RepairJob by job ID.
// Returns a value copy so callers cannot mutate FSM state directly.
// Returns ErrJobNotFound if the job is not in the registry.
func (mfsm *MetadataFSM) GetRepairJob(jobID string) (RepairJob, error) {
	mfsm.jrMutex.RLock()
	defer mfsm.jrMutex.RUnlock()
	job, ok := mfsm.repairJobRegistry[jobID]
	if !ok {
		return RepairJob{}, ErrJobNotFound
	}
	return *job, nil
}

// GetJobsByStatus returns all repair jobs matching the given status.
// Used for crash recovery (e.g. finding all InProgress jobs after
// leader failover).
func (mfsm *MetadataFSM) GetJobsByStatus(status RepairStatus) ([]RepairJob, error) {
	mfsm.jrMutex.RLock()
	defer mfsm.jrMutex.RUnlock()

	var jobs []RepairJob
	for _, job := range mfsm.repairJobRegistry {
		if job.Status == status {
			jobs = append(jobs, *job)
		}
	}
	return jobs, nil
}

// GetPendingJobsForNode returns value copies of all pending repair jobs
// where the given node is the source. Used by the heartbeat handler to
// piggyback repair instructions onto heartbeat responses.
func (mfsm *MetadataFSM) GetPendingJobsForNode(nodeID string) ([]RepairJob, error) {
	mfsm.jrMutex.RLock()
	defer mfsm.jrMutex.RUnlock()

	var jobs []RepairJob
	for _, job := range mfsm.repairJobRegistry {
		if job.Status == RepairStatusPending && job.SourceNodeID == nodeID {
			jobs = append(jobs, *job)
		}
	}
	return jobs, nil
}

// UpdateLastSeen directly updates a node's LastSeen timestamp in memory.
// This is called by the gRPC heartbeat handler and is NOT a Raft operation —
// heartbeats are too frequent for the Raft log. Only the death decision
// (CmdMarkNodeDead) is committed through Raft.
func (mfsm *MetadataFSM) UpdateLastSeen(nodeID string, ts time.Time) error {
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node, ok := mfsm.nodeRegistry[nodeID]
	if !ok {
		return ErrNodeNotFound
	}
	node.LastSeen = ts
	return nil
}

// GetAllNodes returns a value-copy of every NodeEntry in the registry,
// regardless of status. Used by NodeWatcher.sweep() to iterate all nodes
// without holding the lock for the entire sweep duration.
func (mfsm *MetadataFSM) GetAllNodes() []NodeEntry {
	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()

	nodes := make([]NodeEntry, 0, len(mfsm.nodeRegistry))
	for _, node := range mfsm.nodeRegistry {
		nodes = append(nodes, *node)
	}
	return nodes
}

// Proposes the new status through raft
func (mfsm *MetadataFSM) UpdateRepairJobStatus(raft *raft.Raft, jobid string, status RepairStatus) error {
	updateJob := CommandUpdateRepairJob{JobID: jobid, Status: status, UpdatedAt: time.Now()}
	payload, err := json.Marshal(updateJob)
	if err != nil {
		return err
	}
	cmd := MetadataCommand{Type: CmdUpdateRepairJob, Payload: payload}
	return Propose(raft, cmd)
}

func ProposeUpdateNodeSpace(raft *raft.Raft, nodeID string, space uint64, cc uint64) error {
	us := CommandUpdateNodeSpace{
		NodeID:     nodeID,
		FreeSpace:  space,
		ChunkCount: cc,
		UpdatedAt:  time.Now(),
	}
	payload, err := json.Marshal(us)
	if err != nil {
		return err
	}
	cmd := MetadataCommand{
		Type:    CmdUpdateNodeSpace,
		Payload: payload,
	}
	return Propose(raft, cmd)
}
