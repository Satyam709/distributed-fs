package chunk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"log/slog"
	"os"

	dfserrors "github.com/satyam709/distributed-fs/internal/errors"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/store"
)

// Package-level aliases so existing call-sites (tests, server) keep compiling.
var (
	ErrOperationAborted = errors.New("op has been aborted")
	ErrWriterClosed     = errors.New("write over a closed writer")
	ErrChecksumMismatch = dfserrors.ErrChecksumMismatch
)

type ChunkWriter struct {
	filepath string
	file     *os.File
	chunkId  string
	store    store.Store
	isDone   bool
	hasher   hash.Hash
	written  int64
	logger   *logging.CLogger
}

func NewChunkWriter(chunkId string, store store.Store) (*ChunkWriter, error) {
	if chunkId == "" {
		return nil, dfserrors.ErrInvalidChunkId
	}
	if store == nil {
		return nil, errors.New("ChunkWriter: store must not be nil")
	}
	logger := logging.NewCLogger().With(
		slog.String("component", "ChunkWriter"),
		slog.String("chunkId", chunkId),
	)

	tmpPath, err := store.TempDir(chunkId)
	if err != nil {
		return nil, err
	}

	cw := &ChunkWriter{
		chunkId:  chunkId,
		store:    store,
		filepath: tmpPath,
		hasher:   sha256.New(),
		logger:   logger,
	}

	logger.Debug("creating temp file", slog.String("path", cw.filepath))

	f, err := os.Create(cw.filepath)
	if err != nil {
		logger.Error("failed to create temp file", err, slog.String("path", cw.filepath))
		return nil, err
	}
	cw.file = f

	logger.Info("ChunkWriter ready", slog.String("tempPath", cw.filepath))
	return cw, nil
}

func (cw *ChunkWriter) Write(data []byte) error {
	if cw.isDone {
		cw.logger.Debug("Write called on closed writer")
		return ErrWriterClosed
	}

	n, err := cw.file.Write(data)
	if err != nil {
		cw.logger.Error("Write: file write failed", err, slog.Int("attempted", len(data)))
		return err
	}
	cw.hasher.Write(data[:n])
	cw.written += int64(n)

	if syncErr := cw.file.Sync(); syncErr != nil {
		cw.logger.Error("Write: fsync failed", syncErr)
		return syncErr
	}

	cw.logger.Debug("Write: frame flushed",
		slog.Int("frameBytes", n),
		slog.Int64("totalWritten", cw.written),
	)
	return nil
}

// Finalize verifies the checksum and atomically commits the chunk to the store.
// Called on the last frame.
// Calls Aborts if any arror
func (cw *ChunkWriter) Finalize(expectedChecksum []byte) (err error) {
	if cw.isDone {
		return ErrOperationAborted
	}

	computed := cw.hasher.Sum(nil)

	if !bytes.Equal(computed, expectedChecksum) {
		cw.logger.Error("Finalize: checksum mismatch", dfserrors.ErrChecksumMismatch,
			slog.String("computed", hex.EncodeToString(computed)),
			slog.String("expected", hex.EncodeToString(expectedChecksum)),
		)
		cw.Abort()
		return dfserrors.ErrChecksumMismatch
	}

	// If something else fails, still clean up.
	defer func() {
		if err != nil {
			cw.logger.Error("Finalize: commit failed, aborting", err)
			cw.Abort()
		}
	}()

	_ = cw.file.Sync()
	_ = cw.file.Close()
	cw.isDone = true

	err = cw.store.Rename(cw.filepath, cw.chunkId)
	if err != nil {
		return
	}

	cw.logger.Info("Finalize: chunk committed", slog.String("chunkId", cw.chunkId))
	return nil
}

// ComputedChecksum returns the running SHA-256 checksum accumulated so far.
// Safe to call at any point; does not finalise the writer.
func (cw *ChunkWriter) ComputedChecksum() []byte {
	return cw.hasher.Sum(nil)
}

// Abort cleans up the partial temp file when the stream dies mid-transfer.
func (cw *ChunkWriter) Abort() {
	cw.logger.Info("Abort: removing partial temp file", slog.String("path", cw.filepath))
	_ = cw.file.Close()
	_ = os.Remove(cw.filepath)
	cw.isDone = true
}
