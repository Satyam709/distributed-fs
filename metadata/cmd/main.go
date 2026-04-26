package main

import (
	"context"
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

	cfg, err := metadata.LoadConfigDefaultFlow()
	if err != nil {
		logger.FatalError("failed to load config", err)
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
