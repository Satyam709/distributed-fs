package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	cfg := metadata.DefaultNodeConfig()

	configPath := getEnv("METADATA_CONFIG", "")
	if configPath != "" {
		if err := cfg.ApplyJSON(configPath); err != nil {
			fmt.Fprintf(os.Stderr, "failed to load config from %s: %v\n", configPath, err)
			os.Exit(1)
		}
	}

	cfg.ApplyEnv()
	cfg.Default()
	return cfg
}

func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
