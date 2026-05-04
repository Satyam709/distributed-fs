package main

import (
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/internal/retry"
	"github.com/satyam709/distributed-fs/storage"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/store"
)

func main() {
	logger := logging.NewCLogger().With(slog.String("component", "main"))

	logger.Info("distributed-fs storage node starting")

	cfg, err := storage.LoadConfigDefaultFlow()
	if err != nil {
		logger.FatalError("invalid config", err)
	}

	nodeID := cfg.NodeID
	if nodeID == "" {
		nodeID = storage.LoadOrGenerateNodeID(cfg.DataDir, logger)
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

	retryPolicy := retry.Policy{
		MaxAttempts: cfg.RetryMaxAttempts,
		Base:        cfg.RetryBaseBackoff,
		Max:         cfg.RetryMaxBackoff,
		Multiplier:  2.0,
	}
	metaClient, err := metaclient.NewMetadataClient(cfg.MetadataAddrs, retryPolicy, cfg.RPCTimeout)
	if err != nil {
		logger.FatalError("failed to create metadata client", err)
	}
	logger.Info("metadata client ready", slog.Any("metadataAddrs", cfg.MetadataAddrs))

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
