package fsm

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata"
)

type MetadataFSM struct {
	logger            logging.CLogger
	fiMutex           sync.RWMutex
	FileIndex         map[string]metadata.FileRecord
	crMutex           sync.RWMutex
	ChunkRegistry     map[string]metadata.ChunkRecord
	nrMutex           sync.RWMutex
	NodeRegistry      map[string]metadata.NodeEntry
	jrMutex           sync.RWMutex
	RepairJobRegistry map[string]metadata.RepairJob
}

var (
	NodeNotFound  error = errors.New("node does not exist")
	FileNotFound  error = errors.New("file does not exist")
	ChunkNotFound error = errors.New("chunk does not exist")
)

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
	node, err := mfsm.getNodeRegistryForID(req.NodeID)
	if err != nil {
		mfsm.logger.Debug("CmdRegisterNode: node already existis... updating it")

		node.Address = req.Address
		node.Status = metadata.NodeStatusAlive
		node.FreeSpace = req.FreeSpace
		node.ChunkCount = req.ChunkCount
		node.UpdatedAt = req.CreatedAt
	} else {
		mfsm.logger.Debug("CmdRegisterNode: registering new node ", slog.String("id", req.NodeID))
		node = metadata.NodeEntry{
			Address:      req.Address,
			NodeID:       req.NodeID,
			FreeSpace:    req.FreeSpace,
			RegisteredAt: req.CreatedAt,
			UpdatedAt:    req.CreatedAt,
			Status:       metadata.NodeStatusAlive,
			ChunkCount:   req.ChunkCount,
		}
	}
	return upsert(&mfsm.nrMutex, mfsm.NodeRegistry, node.NodeID, node)
}

func (mfsm *MetadataFSM) handleCmdDeregisterNode(req CommandDeregisterNode) error {
	node, err := mfsm.getNodeRegistryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("CmdDeregisterNode failed:"), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.Status = metadata.NodeStatusDraining
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	mfsm.NodeRegistry[req.NodeID] = node
	return nil
}

func (mfsm *MetadataFSM) handleCmdMarkNodeDead(req CommandMarkNodeDead) error {
	node, err := mfsm.getNodeRegistryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("CmdMarkNodeDead failed:"), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.Status = metadata.NodeStatusDead
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	mfsm.NodeRegistry[req.NodeID] = node
	return nil
}

func (mfsm *MetadataFSM) handleCmdMarkNodeAlive(req CommandMarkNodeAlive) error {
	node, err := mfsm.getNodeRegistryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("CmdMarkNodeAlive failed:"), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.Status = metadata.NodeStatusAlive
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	mfsm.NodeRegistry[req.NodeID] = node
	return nil
}
func (mfsm *MetadataFSM) handleCmdUpdateNodeSpace(req CommandUpdateNodeSpace) error {
	node, err := mfsm.getNodeRegistryForID(req.NodeID)
	if err != nil {
		return errors.Join(errors.New("CmdMarkNodeDead failed:"), err)
	}
	mfsm.nrMutex.Lock()
	defer mfsm.nrMutex.Unlock()
	node.FreeSpace = req.FreeSpace
	node.ChunkCount = req.ChunkCount
	if node.UpdatedAt.Before(req.UpdatedAt) {
		node.UpdatedAt = req.UpdatedAt
	}
	mfsm.NodeRegistry[req.NodeID] = node
	return nil
}

// File Cmds

func (mfsm *MetadataFSM) handleCmdCreateFile(req CommandCreateFile) error {
	// see if this fileId is already recorded
	_, err := mfsm.getFileRegistryForID(req.FileID)
	if err == nil {
		// file exists
		return errors.New("CmdCreateFile: fileID already exists in our systems, cannot override")
	}

	file := metadata.FileRecord{
		FileID:    req.FileID,
		Filename:  req.FileName,
		FileSize:  req.FileSize,
		ChunkIDs:  req.ChunkIDs,
		Status:    metadata.FileStatusCreating,
		CreatedAt: req.CreatedAt,
	}

	// create all chunk records
	chunkRecords := make([]metadata.ChunkRecord, len(req.ChunkIDs))

	for i, cid := range req.ChunkIDs {
		chunkRecord := metadata.ChunkRecord{
			ChunkID:    cid,
			FileID:     req.FileID,
			ChunkIndex: i,
			Status:     metadata.ChunkStatusRequestAllocation,
		}
		chunkRecords = append(chunkRecords, chunkRecord)
	}
	// Assumption: since we dont have duplicate fileID
	// than for sure we wouldnot have duplicate chunkIDs
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
	file, err := mfsm.getFileRegistryForID(req.FileID)
	if err != nil {
		return errors.Join(errors.New("CmdCommitFile:"), err)
	}

	if file.FileSize != req.FileSize {
		return errors.New("CmdCommitFile: filesize mismatch")
	}
	if !bytes.Equal(file.CheckSum, req.Checksum) {
		return errors.New("CmdCommitFile: checksum mismatch")
	}

	// validate if all chunks are commited
	mfsm.crMutex.Lock()
	for _, cid := range file.ChunkIDs {
		ck, ok := mfsm.ChunkRegistry[cid]
		if !ok {
			mfsm.crMutex.Unlock()
			err := errors.New("CmdCommitFile: chunk validation failed : chunk not found")
			logging.LogError(&mfsm.logger.Logger, "CmdCommitFile failed", err, "chunk_id", cid)
			return err
		}

		if ck.Status != metadata.ChunkStatusComplete {
			mfsm.crMutex.Unlock()
			err := errors.New("CmdCommitFile: chunk validation failed : chunk not commited")
			logging.LogError(&mfsm.logger.Logger, "CmdCommitFile failed", err, "chunk_id", cid)
			return err
		}
	}
	mfsm.crMutex.Unlock()

	mfsm.fiMutex.Lock()
	defer mfsm.fiMutex.Unlock()
	file.Status = metadata.FileStatusComplete
	mfsm.FileIndex[req.FileID] = file
	return nil
}

func upsert[T any](mu sync.Locker, registry map[string]T, key string, entry T) error {
	if mu != nil {
		// if mu == nil: assume that registry is safe for
		// concurrent use without mutex or its not meant for it

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

func (mfsm *MetadataFSM) getNodeRegistryForID(nodeID string) (metadata.NodeEntry, error) {
	mfsm.nrMutex.RLock()
	defer mfsm.nrMutex.RUnlock()
	node, ok := mfsm.NodeRegistry[nodeID]
	if !ok {
		return metadata.NodeEntry{}, NodeNotFound
	}
	return node, nil
}

func (mfsm *MetadataFSM) getFileRegistryForID(fileID string) (metadata.FileRecord, error) {
	mfsm.fiMutex.RLock()
	defer mfsm.fiMutex.RUnlock()
	file, ok := mfsm.FileIndex[fileID]
	if !ok {
		return metadata.FileRecord{}, FileNotFound
	}
	return file, nil
}
