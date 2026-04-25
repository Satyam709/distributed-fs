package main

import (
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/store"
)

func main() {
	logger := logging.NewCLogger().With(slog.String("component", "main"))

	logger.Info("distributed-fs storage node starting")

	cfg := storage.DefaultStorageNodeConfig()
	cfg.GRPCAddr = ":4000"
	cfg.MetadataAddr = ":3000"

	if err := cfg.Validate(); err != nil {
		logger.FatalError("invalid config", err)
	}

	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = loadOrGenerateNodeID(cfg.DataDir, logger)
		cfg.NodeID = nodeID
	}

	rootDataDir := cfg.DataDir
	logger.Info("resolved data directories",
		slog.String("rootDataDir", rootDataDir),
		slog.String("tempDir", filepath.Join(rootDataDir, "tmp")),
	)

	logger.Info("opening checksum index (BoltDB)")
	boltDb, err := store.NewChecksumIndexBoltDB[[]byte](store.ByteCodec{},
		store.WithDbPath[[]byte](rootDataDir))

	if err != nil {
		logger.FatalError("failed to init checksum-store", err)
	}

	if err := boltDb.Open(); err != nil {
		logger.FatalError("failed to open checksum-store", err)
	}
	logger.Info("checksum index opened")

	logger.Info("creating disk store")
	diskStore, err := store.NewDiskStore(
		store.WithChecksumStore(boltDb),
		store.WithRootDir(rootDataDir),
		store.WithTempDir(filepath.Join(rootDataDir, "tmp")),
		store.WithDiskStoreLogger(logger),
	)
	if err != nil {
		logger.FatalError("failed to create disk store", err)
	}
	logger.Info("disk store ready", slog.String("rootDir", rootDataDir))

	metaClient, err := metaclient.NewMetadataClient(cfg.MetadataAddr)
	if err != nil {
		logger.FatalError("failed to create metadata client", err)
	}
	logger.Info("metadata client ready", slog.String("metadataAddr", cfg.MetadataAddr))

	node, err := storage.NewStorageNode(*cfg, logger, diskStore, metaClient)
	if err != nil {
		logger.FatalError("failed to create storage node", err)
	}

	if err := node.Start(); err != nil {
		logger.FatalError("failed to start storage node", err)
	}
	logger.Info("storage node started",
		slog.String("nodeID", cfg.NodeID),
		slog.String("grpcAddr", cfg.GRPCAddr),
	)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	logger.Info("shutdown signal received", slog.String("signal", sig.String()))

	node.Stop()
	boltDb.CleanUp()
	logger.Info("storage node shut down cleanly")
}

func loadOrGenerateNodeID(dataDir string, logger *logging.CLogger) string {
	nodeIDPath := filepath.Join(dataDir, "node_id")

	data, err := os.ReadFile(nodeIDPath)
	if err == nil && len(data) > 0 {
		nodeID := string(data)
		logger.Info("loaded existing node_id", slog.String("nodeID", nodeID))
		return nodeID
	}

	nodeID := uuid.New().String()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.FatalError("failed to create data directory", err)
	}
	if err := os.WriteFile(nodeIDPath, []byte(nodeID), 0644); err != nil {
		logger.FatalError("failed to write node_id file", err)
	}
	logger.Info("generated new node_id", slog.String("nodeID", nodeID))
	return nodeID
}
