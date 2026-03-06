package chunk

import (
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"os"

	errordfs "github.com/satyam709/distributed-fs/internal/errors"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/store"
)

const DefaultFrameSize = 32 * 1024 // 32 KiB — one frame over the stream

// ErrChunkNotFound is returned by NewChunkReader when the chunk does not exist.
var ErrChunkNotFound = errordfs.ErrChunkNotFound

// Frame is one unit of streaming data returned by ChunkReader.Next.
//
// Checksum is the SHA-256 of this frame's Data only (not cumulative across
// frames). The receiver can verify each frame independently as it arrives
// rather than buffering the whole chunk — providing immediate corruption
// detection mid-stream.
type Frame struct {
	Data     []byte
	Checksum []byte // SHA-256(Data) for this frame only
	IsLast   bool
}

// ChunkReader streams a committed chunk from disk in fixed-size frames.
//
// Usage:
//
//	r, err := NewChunkReader(id, store, 0)
//	defer r.Close()
//	for {
//	    frame, err := r.Next()
//	    if err == io.EOF { break }
//	    // process frame ...
//	    if frame.IsLast { break }
//	}
type ChunkReader struct {
	chunkId   string
	file      *os.File
	frameSize int
	logger    *logging.CLogger
}

// NewChunkReader opens the chunk identified by chunkId from s for reading.
//
//   - Returns ErrChunkNotFound when the chunk is absent from the store.
//   - Pass frameSize=0 to use the default frame size (32 KiB per the design doc).
//   - The caller must call Close when done to release the file descriptor.
func NewChunkReader(chunkId string, s store.Store, frameSize int) (*ChunkReader, error) {
	if chunkId == "" {
		return nil, store.InvalidChunkId
	}
	if s == nil {
		return nil, errors.New("ChunkReader: store must not be nil")
	}

	// Existence check before touching the filesystem — store.Exists is a fast
	// index lookup (BoltDB) and avoids leaking file descriptors on the hot path.
	if !s.Exists(chunkId) {
		return nil, ErrChunkNotFound
	}

	absChunkPath, err := s.PathForChunk(chunkId)
	if err != nil {
		return nil, err
	}

	logger := logging.NewCLogger()
	logger.Logger = *logger.Logger.With(
		slog.String("component", "ChunkReader"),
		slog.String("chunkId", chunkId),
	)

	if frameSize <= 0 {
		frameSize = DefaultFrameSize
	}

	f, err := os.Open(absChunkPath)
	if err != nil {
		logger.Error("NewChunkReader: failed to open chunk file", err,
			slog.String("path", absChunkPath))
		return nil, err
	}

	logger.Info("ChunkReader ready", slog.String("path", absChunkPath))
	return &ChunkReader{
		chunkId:   chunkId,
		file:      f,
		frameSize: frameSize,
		logger:    logger,
	}, nil
}

// Next reads and returns the next frame from the chunk.
//
// Return values:
//   - (frame, nil)       — a valid frame; check frame.IsLast to know if it
//     is the final one.
//   - (Frame{}, io.EOF)  — the stream is fully consumed (no bytes left).
//   - (Frame{}, err)     — an I/O error occurred; the reader should be closed.
func (cr *ChunkReader) Next() (Frame, error) {
	// Read one extra byte beyond the frame size.  If the file had exactly
	// frameSize bytes left, the OS returns n==frameSize with err==io.EOF (or
	// nil, followed by EOF on the next call); either way n < frameSize+1 so
	// IsLast is true.  If the file has more data, n==frameSize+1 and we seek
	// the extra byte back before returning.
	buf := make([]byte, cr.frameSize+1)
	n, err := cr.file.Read(buf)

	// A genuine I/O error alongside a partial read — propagate immediately.
	if err != nil && err != io.EOF {
		return Frame{}, err
	}

	// Nothing read: either the stream is exhausted (io.EOF) or a real error.
	if n == 0 {
		return Frame{}, err
	}
	// n <= frameSize  →  EOF was reached inside this read (last frame).
	// n == frameSize+1 →  more data exists; undo the one-byte lookahead.
	isLast := n <= cr.frameSize
	data := buf[:min(n, cr.frameSize)]

	if !isLast {
		if _, seekErr := cr.file.Seek(-1, io.SeekCurrent); seekErr != nil {
			cr.Close()
			return Frame{}, seekErr
		}
	}

	h := sha256.Sum256(data)
	cr.logger.Debug("Next: frame dispatched",
		slog.Int("frameBytes", len(data)),
		slog.Bool("isLast", isLast),
	)
	return Frame{Data: data, Checksum: h[:], IsLast: isLast}, nil
}

// Close releases the underlying file descriptor.  Safe to call multiple times.
func (cr *ChunkReader) Close() {
	if cr.file != nil {
		_ = cr.file.Close()
		cr.file = nil
	}
}
