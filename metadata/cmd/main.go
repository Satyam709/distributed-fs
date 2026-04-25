package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/metadata"
)

func main() {
	logger := logging.NewCLogger().With(slog.String("component", "main"))

	logger.Info("distributed-fs metadata node starting")

	cfg := loadConfig()
	err := cfg.Validate()
	if err != nil {
		logger.FatalError("failed to validate config: ", err)
	}

	app, err := metadata.NewMetadataApp(cfg)
	if err != nil {
		logger.Error("failed to create metadata app", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.Run(ctx); err != nil {
		logger.Error("failed to start metadata app", err)
		os.Exit(1)
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	sig := <-signalChannel
	logger.Info("shutdown signal received", "signal", sig.String())

	if err := app.Shutdown(context.Background()); err != nil {
		logger.Error("failed to shutdown metadata app", err)
		os.Exit(1)
	}

	logger.Info("metadata node shut down cleanly")
}

func loadConfig() metadata.NodeConfig {
	pwd, _ := os.Getwd()
	configPath := getEnv("METADATA_CONFIG", "")
	if configPath != "" {
		cfg, err := metadata.LoadFromJSON(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load config from %s: %v\n", configPath, err)
			os.Exit(1)
		}
		return *cfg
	}

	nodeID := getEnv("METADATA_NODE_ID", "node-1")
	grpcAddr := getEnv("METADATA_GRPC_ADDR", ":4001")
	raftAddr := getEnv("METADATA_RAFT_ADDR", "127.0.0.1:5001")

	raftDir := getEnv("METADATA_RAFT_DIR", filepath.Join(pwd, "data", "metadata", "raft"))

	peerAddrs := parsePeerAddrs(getEnv("METADATA_PEER_ADDRS", ""))

	bootstrap := getEnv("METADATA_BOOTSTRAP", "false") == "true"

	cfg := metadata.NodeConfig{
		NodeID:    nodeID,
		GRPCAddr:  grpcAddr,
		RaftAddr:  raftAddr,
		RaftDir:   raftDir,
		PeerAddrs: peerAddrs,
		Bootstrap: bootstrap,
	}

	cfg.ReplicationFactor = getEnvInt("METADATA_REPLICATION_FACTOR", 3)
	cfg.SuspectTimeout = getEnvDuration("METADATA_SUSPECT_TIMEOUT", 10)
	cfg.DeadTimeout = getEnvDuration("METADATA_DEAD_TIMEOUT", 30)
	cfg.WatcherInterval = getEnvDuration("METADATA_WATCHER_INTERVAL", 5)
	cfg.ReconcileDelay = getEnvDuration("METADATA_RECONCILE_DELAY", 30)
	cfg.HeartbeatTimeout = getEnvDuration("METADATA_HEARTBEAT_TIMEOUT", 1)
	cfg.ElectionTimeout = getEnvDuration("METADATA_ELECTION_TIMEOUT", 5)
	cfg.SnapshotInterval = getEnvDuration("METADATA_SNAPSHOT_INTERVAL", 30)
	cfg.SnapshotThreshold = uint64(getEnvInt("METADATA_SNAPSHOT_THRESHOLD", 8192))
	cfg.SnapshotRetain = getEnvInt("METADATA_SNAPSHOT_RETAIN", 2)

	// try to get defaults if something absent
	cfg.Default()
	return cfg
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			return val
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultSeconds int) time.Duration {
	if v := os.Getenv(key); v != "" {
		// Try parsing as duration string (e.g., "30s", "1m")
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		// Try parsing as seconds
		if sec, err := strconv.Atoi(v); err == nil {
			return time.Duration(sec) * time.Second
		}
	}
	return time.Duration(defaultSeconds) * time.Second
}

func parsePeerAddrs(s string) map[string]string {
	if s == "" {
		return nil
	}
	result := make(map[string]string)
	// Format: node1:addr1,node2:addr2
	// For now just return empty - can be enhanced later
	_ = fmt.Sprint(s)
	return result
}
