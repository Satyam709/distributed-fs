package metadata

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"google.golang.org/grpc"
)

type MetadataNode struct {
	Config       NodeConfig
	logger       *logging.CLogger
	stableStore  raft.StableStore
	logStore     raft.LogStore
	fsm          raft.FSM
	raftInstance *raft.Raft
	server       *grpc.Server
}

// init all dependencies
func NewMetadataNode(config NodeConfig) (*MetadataNode, error) {

	fsm := fsm.NewEmptyMetadataFsm(logging.NewCLogger())

	info, err := os.Stat(config.RaftDir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("NewMetadataNode: raftDir doesnt not exist")
	}

	// setup stable and logstore
	boltStore, err := raftboltdb.New(
		raftboltdb.Options{
			Path: filepath.Join(config.RaftDir, "raft.db"),
		},
	)
	if err != nil {
		return nil, err
	}

	stableStore := boltStore
	logStore := boltStore

	// init raft
	raftIns, err := NewRaftNode(RaftConfig{
		Config:      config,
		FSM:         fsm,
		LogStore:    logStore,
		StableStore: stableStore,
	})
	if err != nil {
		return nil, err
	}

	// TODO: add grpc server

	return &MetadataNode{
		Config:       config,
		logger:       logging.NewCLogger().With("Component", "MetadataNode"),
		stableStore:  stableStore,
		logStore:     logStore,
		fsm:          fsm,
		raftInstance: raftIns,
	}, nil
}

func (mn *MetadataNode) Start(ctx context.Context) error {
	mn.logger.Info("Starting up node")
	panic("unimplemented")
}

func (mn *MetadataNode) Shutdown(ctx context.Context) error {
	mn.logger.Info("stoping node")
	panic("unimplemented")
}
