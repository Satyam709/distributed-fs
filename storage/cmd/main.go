package main

import (
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage"
	"github.com/satyam709/distributed-fs/storage/store"
)

func main() {
	logger := logging.NewCLogger().With(slog.String("component", "main"))

	logger.Info("distributed-fs storage node starting")

	// Resolve working directory.
	pwd, err := os.Getwd()
	if err != nil {
		logger.FatalError("failed to get working directory", err)
	}
	rootDataDir := filepath.Join(pwd, "data")
	logger.Info("resolved data directories",
		slog.String("rootDataDir", rootDataDir),
		slog.String("tempDir", filepath.Join(rootDataDir, "tmp")),
	)

	// Open checksum index (BoltDB).
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

	// Create disk store.
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

	// Start storage node.
	config := storage.StorageNodeConfig{Port: ":4000", Timeout: 120 * time.Second}
	node, err := storage.NewStorageNode(config, logger, diskStore)
	if err != nil {
		logger.FatalError("failed to create storage node", err)
	}

	if err := node.Start(); err != nil {
		logger.FatalError("failed to start storage node", err)
	}
	logger.Info("storage node started", slog.String("port", config.Port))

	// Wait for shutdown signal and clean up.
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	logger.Info("shutdown signal received", slog.String("signal", sig.String()))

	node.Stop()
	boltDb.CleanUp()
	logger.Info("storage node shut down cleanly")
}
