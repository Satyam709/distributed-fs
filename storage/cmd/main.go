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
	logger := logging.NewCLogger()
	logger.Logger = *logger.Logger.With(slog.String("component", "main"))

	logger.Info("distributed-fs storage node starting")

	// ── Checksum Index (BoltDB) ──────────────────────────────────────────────
	logger.Info("opening checksum index (BoltDB)")
	boltDb := store.NewChecksumIndexBoltDB[[32]byte]()
	if err := boltDb.Open(); err != nil {
		logger.FatalError("failed to open checksum-store", err)
	}
	logger.Info("checksum index opened")

	// ── Working Directory ────────────────────────────────────────────────────
	pwd, err := os.Getwd()
	if err != nil {
		logger.FatalError("failed to get working directory", err)
	}
	rootDataDir := filepath.Join(pwd, "data")
	logger.Info("resolved data directories",
		slog.String("rootDataDir", rootDataDir),
		slog.String("tempDir", filepath.Join(rootDataDir, "tmp")),
	)

	// ── Disk Store ───────────────────────────────────────────────────────────
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

	// ── Storage Node ─────────────────────────────────────────────────────────
	config := storage.StorageNodeConfig{Port: ":4000", Timeout: 120 * time.Second}
	node := storage.NewStorageNode(config, logger, diskStore)

	if err := node.Start(); err != nil {
		logger.FatalError("failed to start storage node", err)
	}
	logger.Info("storage node started", slog.String("port", config.Port))

	// ── Graceful Shutdown ────────────────────────────────────────────────────
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	logger.Info("shutdown signal received", slog.String("signal", sig.String()))

	node.Stop()
	boltDb.CleanUp()
	logger.Info("storage node shut down cleanly")
}
