package fsm

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
)

type MetadataFSM struct {
	logger            *logging.CLogger
	fiMutex           sync.RWMutex
	FileIndex         map[string]*FileRecord
	crMutex           sync.RWMutex
	ChunkRegistry     map[string]*ChunkRecord
	nrMutex           sync.RWMutex
	NodeRegistry      map[string]*NodeEntry
	jrMutex           sync.RWMutex
	RepairJobRegistry map[string]*RepairJob
}

var (
	ErrNodeNotFound  = errors.New("node does not exist")
	ErrFileNotFound  = errors.New("file does not exist")
	ErrChunkNotFound = errors.New("chunk does not exist")
	ErrJobNotFound   = errors.New("job does not exist")
)

func NewEmptyMetadataFsm(logger *logging.CLogger) *MetadataFSM {
	return &MetadataFSM{
		logger:            logger,
		FileIndex:         map[string]*FileRecord{},
		ChunkRegistry:     map[string]*ChunkRecord{},
		NodeRegistry:      map[string]*NodeEntry{},
		RepairJobRegistry: map[string]*RepairJob{},
	}
}

func (mfsm *MetadataFSM) Apply(rlog *raft.Log) interface{} {

	return nil
}

func (mfsm *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (mfsm *MetadataFSM) Restore(snapshot io.ReadCloser) error {
	return nil
}

// Command Handlers

func (mfsm *MetadataFSM) handleCmdRegisterNode(req CommandRegisterNode) error {
	node, err := mfsm.getNodeEntryForID(req.NodeID)
	if err == nil {
		mfsm.logger.Debug("CmdRegisterNode: node already existis... updating it")

		node.Address = req.Address
		node.Status = NodeStatusAlive
		node.FreeSpace = req.FreeSpace
		node.ChunkCount = req.ChunkCount
		node.UpdatedAt = req.CreatedAt
	} else {
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
func (mfsm *MetadataFSM) handleCmdUpdateNodeSpace(req CommandUpdateNodeSpace) error {
	node, err := mfsm.getNodeEntryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("cmdMarkNodeDead failed: "), err)
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

// File Cmds

func (mfsm *MetadataFSM) handleCmdCreateFile(req CommandCreateFile) error {
	// see if this fileId is already recorded
	_, err := mfsm.getFileEntryForID(req.FileID)
	if err == nil {
		// file exists
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

	// create all chunk records
	chunkRecords := make([]*ChunkRecord, 0, len(req.ChunkIDs))

	for i, cid := range req.ChunkIDs {
		chunkRecord := &ChunkRecord{
			ChunkID:    cid,
			FileID:     req.FileID,
			ChunkIndex: i,
			Status:     ChunkStatusRequestAllocation,
		}
		chunkRecords = append(chunkRecords, chunkRecord)
	}
	// Assumption: since we don't have duplicate fileID
	// than for sure we wouldn't have duplicate chunkIDs
	// so cases like a chunkID already exists should practically never occur.

	mfsm.crMutex.Lock()
	// pass1: just reconfirm that we dont have these chunks
	for _, val := range chunkRecords {
		_, ok := mfsm.ChunkRegistry[val.ChunkID]
		if ok {
			mfsm.crMutex.Unlock()
			return errors.New("CmdCreateFile: chunkID already exists in our systems, unsupported")
		}
	}

	// pass2: insert them
	for _, val := range chunkRecords {
		mfsm.ChunkRegistry[val.ChunkID] = val
	}
	mfsm.crMutex.Unlock()

	mfsm.fiMutex.Lock()
	defer mfsm.fiMutex.Unlock()
	mfsm.FileIndex[req.FileID] = file

	return nil
}

func (mfsm *MetadataFSM) handleCmdCommitFile(req CommandCommitFile) error {
	// see if this fileId is already recorded
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

	// validate if all chunks are commited
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

func (mfsm *MetadataFSM) handleCmdDeleteFile(req CommandDeleteFile) error {
	// see if this fileId is already recorded
	file, err := mfsm.getFileEntryForID(req.FileID)
	if err != nil {
		return errors.Join(errors.New("cmdDeleteFile: "), err)
	}

	mfsm.fiMutex.Lock()
	defer mfsm.fiMutex.Unlock()

	file.Status = FileStatusDeleted
	return nil
}

func (mfsm *MetadataFSM) handleCmdCommitChunk(req CommandCommitChunk) error {
	chunk, err := mfsm.getChunkEntryForID(req.ChunkID)
	if err != nil {
		return errors.Join(errors.New("cmdCommitChunk: "), err)
	}
	if len(chunk.Checksum) == 0 && chunk.Status != ChunkStatusComplete {
		mfsm.crMutex.Lock()
		chunk.Status = ChunkStatusComplete
		chunk.Checksum = req.Checksum
		chunk.Replicas = req.NodeIDs // although this might be written by placement strategy worker
		mfsm.crMutex.Unlock()
		return nil
	}
	return errors.New("cmdCommitChunk: chunk validation failed")
}

func (mfsm *MetadataFSM) handleCmdEvictChunkFromNode(req CommandEvictChunkFromNode) error {
	chunk, err := mfsm.getChunkEntryForID(req.ChunkID)
	if err != nil {
		return errors.Join(errors.New("cmdEvictChunk: "), err)
	}
	mfsm.crMutex.Lock()
	defer mfsm.crMutex.Unlock()

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

// JOB cmds

func (mfsm *MetadataFSM) handleCmdCreateRepairJob(req CommandCreateRepairJob) error {
	_, err := mfsm.getJobEntryForID(req.JobID)
	if err == nil {
		return errors.New("cmdCreateRepairJob: job already exists")
	}

	// leave this simple for now
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
func (mfsm *MetadataFSM) handleCmdUpdateRepairJob(req CommandUpdateRepairJob) error {
	job, err := mfsm.getJobEntryForID(req.JobID)
	if err != nil {
		return errors.Join(errors.New("cmdUpdateRepairJob: "), err)
	}
	mfsm.crMutex.Lock()
	defer mfsm.crMutex.Unlock()
	job.Status = req.Status
	job.UpdatedAt = req.UpdatedAt
	job.Error = req.Error
	job.Attempts = req.Attempts
	return nil
}

func upsert[T any](mu sync.Locker, registry map[string]T, key string, entry T) error {
	if mu != nil {
		// if mu == nil: assume that registry is safe for
		// concurrent use without mutex, or it's not meant for it

		// else acquire the lock
		mu.Lock()
		defer mu.Unlock()
	}
	if registry == nil {
		return errors.New("upsert: registry is nil")
	}
	// overwrite the registry with given entry
	registry[key] = entry
	return nil
}

func (mfsm *MetadataFSM) getNodeEntryForID(nodeID string) (*NodeEntry, error) {
	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()
	node, ok := mfsm.NodeRegistry[nodeID]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return node, nil
}

func (mfsm *MetadataFSM) getFileEntryForID(fileID string) (*FileRecord, error) {
	mfsm.fiMutex.RLock()
	defer mfsm.fiMutex.RUnlock()
	file, ok := mfsm.FileIndex[fileID]
	if !ok {
		return nil, ErrFileNotFound
	}
	return file, nil
}

func (mfsm *MetadataFSM) getChunkEntryForID(chunkID string) (*ChunkRecord, error) {
	mfsm.crMutex.RLock()
	defer mfsm.crMutex.RUnlock()
	chunk, ok := mfsm.ChunkRegistry[chunkID]
	if !ok {
		return nil, ErrChunkNotFound
	}
	return chunk, nil
}

func (mfsm *MetadataFSM) getJobEntryForID(jobID string) (*RepairJob, error) {
	mfsm.jrMutex.RLock()
	defer mfsm.jrMutex.RUnlock()
	job, ok := mfsm.RepairJobRegistry[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}
	return job, nil
}
