package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type loggerKey struct{}

// A custom logger
type CLogger struct {
	slog.Logger
}

// NewConsoleLogger creates a new logger that writes to the console
func NewConsoleLogger() *slog.Logger {
	handler := getConsoleHandler()
	return slog.New(handler)
}

// NewCLogger creates a new custom logger that writes to the console
func NewCLogger() *CLogger {
	handler := getConsoleHandler()
	return &CLogger{Logger: *slog.New(handler)}
}

// WithLogger adds the logger to the context
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// CtxLogger gives the logger from ctx if it exists, otherwise returns the default logger
func CtxLogger(ctx context.Context) *slog.Logger {
	if ctx.Value(loggerKey{}) != nil {
		return ctx.Value(loggerKey{}).(*slog.Logger)
	}
	return slog.Default()
}

// LogError logs an error message
func LogError(logger *slog.Logger, msg string, err error, args ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	args = append(args, "error", err)
	logger.Error(msg, args...)
}

// LogFatalError logs an error message and exits the program
func LogFatalError(logger *slog.Logger, msg string, err error, args ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	args = append(args, "error", err)
	logger.Error(msg, args...)
	os.Exit(1)
}

// LogFatal logs a message and exits the program
func LogFatal(logger *slog.Logger, msg string, args ...any) {
	if logger == nil {
		logger = slog.Default()
	}
	logger.Error(msg, args...)
	os.Exit(1)
}

// Error logs an error message on CLogger
func (c *CLogger) Error(msg string, err error, args ...any) {
	LogError(&c.Logger, msg, err, args...)
}

// FatalError logs an error message and exits the program on CLogger
func (c *CLogger) FatalError(msg string, err error, args ...any) {
	LogFatalError(&c.Logger, msg, err, args...)
}

// Fatal logs a message and exits the program on CLogger
func (c *CLogger) Fatal(msg string, args ...any) {
	LogFatal(&c.Logger, msg, args...)
}

func getConsoleHandler() slog.Handler {
	handlerOptions := &slog.HandlerOptions{
		Level: getLogLevel(),
	}
	return slog.NewTextHandler(os.Stdout, handlerOptions)
}

func getLogLevel() slog.Level {
	level := os.Getenv("LOG_LEVEL")
	switch strings.ToUpper(level) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
