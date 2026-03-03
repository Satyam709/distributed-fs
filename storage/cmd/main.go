package main

import (
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
	// Application Logger
	logger := logging.NewCLogger()

	// CheckSumIndexStore
	boldDb := store.NewChecksumIndexBoltDB[[32]byte]()
	err := boldDb.Open()
	if err != nil {
		logger.FatalError("failed to open the checksum-store", err)
	}

	// get the current dir
	pwd, err := os.Getwd()
	if err != nil {
		logger.FatalError("failed to get pwd :(", err)
	}

	rootDataDir := filepath.Join(pwd, "data")

	// Create the diskStore
	diskStore, err := store.NewDiskStore(
		store.WithChecksumStore(boldDb),
		store.WithRootDir(rootDataDir),
		store.WithTempDir(filepath.Join(rootDataDir, "tmp")))

	if err != nil {
		logger.FatalError("cannot create diskStore", err)
	}

	// Configure the server
	config := storage.StorageNodeConfig{Port: ":4000", Timeout: 120 * time.Second}

	node := storage.NewStorageNode(config, logger, diskStore)

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	// block untill signal
	<-signalChannel

	// gracefully shutdown server
	node.Stop()
	boldDb.CleanUp()
}
