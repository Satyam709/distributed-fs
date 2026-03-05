package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	dfserrors "github.com/satyam709/distributed-fs/internal/errors"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/chunk"
	"github.com/satyam709/distributed-fs/storage/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StorageServer implements the StorageServiceServer gRPC interface.
type StorageServer struct {
	pb_storage.UnimplementedStorageServiceServer
	Store  store.Store
	logger *logging.CLogger
}

// NewStorageServer creates a StorageServer with a component-scoped logger.
// Returns an error if s is nil.
func NewStorageServer(s store.Store, logger *logging.CLogger) (*StorageServer, error) {
	if s == nil {
		return nil, errors.New("StorageServer: Store must not be nil")
	}
	l := logging.NewCLogger()
	if logger != nil {
		l = logger
	}
	l.Logger = *l.Logger.With(slog.String("component", "StorageServer"))
	return &StorageServer{Store: s, logger: l}, nil
}

// PutChunk receives a client-streaming RPC that delivers chunk data in
// sequential frames and atomically commits the chunk once all frames have
// been received and the final checksum is verified.
//
// Protocol:
//   - Each request frame carries chunk_id, a data payload, a cumulative
//     SHA-256 checksum, and an is_last flag.
//   - The checksum on each frame covers all bytes received so far (cumulative),
//     so the final frame's checksum is the SHA-256 of the entire chunk.
//   - All frames in a single stream must share the same chunk_id.
//   - The server creates a ChunkWriter on the first frame, writes each frame
//     to a temporary file, then on EOF atomically renames it to the final
//     chunk path after verifying the cumulative checksum.
//
// Error codes:
//   - InvalidArgument — empty stream, stream closed before is_last, or
//     chunk_id mismatch across frames.
//   - Internal        — writer initialisation, frame write, or finalize failure.
func (s *StorageServer) PutChunk(stream grpc.ClientStreamingServer[pb_storage.PutChunkRequest, pb_storage.PutChunkResponse]) error {
	var writer *chunk.ChunkWriter
	var registeredChunkId string

	s.logger.Info("PutChunk: stream opened")
	var lastFrame *pb_storage.PutChunkRequest

	for {
		frame, err := stream.Recv()

		// EOF: client closed the send side.
		if err == io.EOF {
			// No frames were received at all — malformed call.
			if writer == nil {
				s.logger.Error("PutChunk: EOF before any frame received", dfserrors.ErrInvalidChunkId)
				return status.Error(codes.InvalidArgument, "no data received")
			}

			// Stream closed before the client set is_last — treat as broken.
			if lastFrame == nil || !lastFrame.IsLast {
				s.logger.Error("PutChunk: stream closed without is_last flag", dfserrors.ErrInvalidChunkId)
				writer.Abort()
				return status.Error(codes.InvalidArgument, "stream closed before final frame")
			}

			// Finalize: verify cumulative checksum and atomically commit.
			// lastFrame.Checksum is the SHA-256 of the entire chunk content
			// accumulated across all frames.  ChunkWriter compares it against
			// its own running hash and renames the .tmp file to .chunk on match.
			s.logger.Info("PutChunk: finalising chunk", slog.String("chunkId", lastFrame.ChunkId))

			if finalizeErr := writer.Finalize(lastFrame.Checksum); finalizeErr != nil {
				s.logger.Error("PutChunk: finalize failed", finalizeErr,
					slog.String("chunkId", lastFrame.ChunkId))
				return status.Error(codes.Internal, "failed to finalize chunk")
			}

			s.logger.Info("PutChunk: chunk stored", slog.String("chunkId", lastFrame.ChunkId))
			// Echo the chunk_id and the client-supplied checksum back so the
			// caller can confirm the correct chunk was committed.
			return stream.SendAndClose(&pb_storage.PutChunkResponse{
				Response: &pb_storage.Response{
					Code: 200,
					Msg:  "Chunk Stored",
				},
				ChunkId:  lastFrame.ChunkId,
				Checksum: lastFrame.Checksum,
			})
		}

		// Transport error.
		if err != nil {
			s.logger.Error("PutChunk: stream recv error", err)
			if writer != nil {
				writer.Abort() // delete the partial .tmp file
			}
			return status.Error(codes.Internal, "stream error")
		}

		// First frame: open a ChunkWriter for this chunk.
		// ChunkWriter creates the .tmp file and begins accumulating the
		// running SHA-256 hash that will be compared against the final checksum.
		if writer == nil {
			s.logger.Debug("PutChunk: first frame, opening writer",
				slog.String("chunkId", frame.ChunkId))

			writer, err = chunk.NewChunkWriter(frame.ChunkId, s.Store)
			if err != nil {
				s.logger.Error("PutChunk: failed to open writer", err,
					slog.String("chunkId", frame.ChunkId))
				return status.Error(codes.Internal, "failed to initialise chunk writer")
			}
			registeredChunkId = frame.ChunkId
		}

		// Reject frames that switch chunk_id mid-stream.
		// All frames in a stream must target the same chunk.  A mismatch
		// indicates a client bug or a multiplexed stream that we do not support.
		if registeredChunkId != frame.ChunkId {
			s.logger.Error("PutChunk: chunk_id changed mid-stream", dfserrors.ErrInvalidChunkId,
				slog.String("registered", registeredChunkId),
				slog.String("received", frame.ChunkId),
			)
			writer.Abort()
			return status.Error(codes.InvalidArgument, "chunk-id mismatch across frames")
		}

		// Write frame payload.
		s.logger.Debug("PutChunk: writing frame",
			slog.String("chunkId", frame.ChunkId),
			slog.Int("frameBytes", len(frame.Data)),
		)

		if writeErr := writer.Write(frame.Data); writeErr != nil {
			s.logger.Error("PutChunk: frame write failed", writeErr,
				slog.String("chunkId", frame.ChunkId))
			writer.Abort()
			return status.Error(codes.Internal, "failed to write chunk frame")
		}

		lastFrame = frame
	}
}

// GetChunk reads the requested chunk from the store and streams it to the client
// in 32 KiB frames. Each frame carries its own SHA-256 checksum so the client
// can detect corruption during transit.
//
// Error codes:
//   - InvalidArgument — empty chunk_id
//   - NotFound        — chunk does not exist in the store
//   - Internal        — I/O error while reading / streaming
func (s *StorageServer) GetChunk(req *pb_storage.GetChunkRequest, stream grpc.ServerStreamingServer[pb_storage.GetChunkResponse]) error {
	chunkId := req.GetChunkId()
	s.logger.Info("GetChunk: called", slog.String("chunkId", chunkId))

	if chunkId == "" {
		s.logger.Error("GetChunk: empty chunk_id", dfserrors.ErrInvalidChunkId)
		return status.Error(codes.InvalidArgument, "chunk_id must not be empty")
	}

	if !s.Store.Exists(chunkId) {
		s.logger.Error("GetChunk: chunk not found", dfserrors.ErrChunkNotFound,
			slog.String("chunkId", chunkId))
		return status.Error(codes.NotFound, "chunk not found")
	}

	reader, err := chunk.NewChunkReader(chunkId, s.Store, 0 /* default 32 KiB */)
	if err != nil {
		if errors.Is(err, chunk.ErrChunkNotFound) {
			return status.Error(codes.NotFound, "chunk not found")
		}
		s.logger.Error("GetChunk: failed to create reader", err,
			slog.String("chunkId", chunkId))
		return status.Error(codes.Internal, "failed to open chunk for reading")
	}
	defer reader.Close()

	s.logger.Info("GetChunk: streaming chunk", slog.String("chunkId", chunkId))

	for {
		frame, nextErr := reader.Next()
		if nextErr == io.EOF {
			// All frames delivered; stream is complete.
			break
		}
		if nextErr != nil {
			s.logger.Error("GetChunk: reader.Next failed", nextErr,
				slog.String("chunkId", chunkId))
			return status.Error(codes.Internal, "error reading chunk data")
		}

		resp := &pb_storage.GetChunkResponse{
			ChunkId:  chunkId,
			Data:     frame.Data,
			Checksum: frame.Checksum,
			IsLast:   frame.IsLast,
		}

		if sendErr := stream.Send(resp); sendErr != nil {
			s.logger.Error("GetChunk: stream send failed", sendErr,
				slog.String("chunkId", chunkId))
			return status.Error(codes.Internal, "failed to send chunk frame")
		}

		if frame.IsLast {
			break
		}
	}

	s.logger.Info("GetChunk: done", slog.String("chunkId", chunkId))
	return nil
}

// DeleteChunk removes a chunk from the store. The operation is idempotent:
// deleting a non-existent chunk is treated as success (Success=true).
//
// Error codes:
//   - InvalidArgument — empty chunk_id
//   - Internal        — unexpected I/O error during deletion
func (s *StorageServer) DeleteChunk(req *pb_storage.DeleteChunkRequest, stream grpc.ServerStreamingServer[pb_storage.DeleteChunkResponse]) error {
	chunkId := req.GetChunkId()
	s.logger.Info("DeleteChunk: called", slog.String("chunkId", chunkId))

	if chunkId == "" {
		s.logger.Error("DeleteChunk: empty chunk_id", dfserrors.ErrInvalidChunkId)
		return status.Error(codes.InvalidArgument, "chunk_id must not be empty")
	}

	err := s.Store.Delete(chunkId)
	if err != nil && !errors.Is(err, dfserrors.ErrChunkNotFound) {
		s.logger.Error("DeleteChunk: delete failed", err,
			slog.String("chunkId", chunkId))
		return status.Error(codes.Internal, "failed to delete chunk")
	}

	// Idempotent — ErrChunkNotFound is treated as success.
	s.logger.Info("DeleteChunk: success", slog.String("chunkId", chunkId))
	return stream.Send(&pb_storage.DeleteChunkResponse{
		Response: &pb_storage.Response{
			Code: 200,
			Msg:  "Chunk Deleted",
		},
		Success: true,
	})
}

// VerifyChunk checks the integrity of the named chunk.
//
// Behaviour:
//   - The server always runs an internal verify (recompute SHA-256 vs stored
//     checksum index). A mismatch yields IsValid=false.
//   - If req.Checksum is additionally non-empty, the recomputed SHA-256 is
//     also compared against the caller-supplied value. A mismatch yields
//     IsValid=false but is NOT returned as an RPC error — corruption is a
//     data-level result, not a protocol error.
//   - NotFound is returned only if the chunk does not exist at all.
//
// Error codes:
//   - InvalidArgument — empty chunk_id
//   - NotFound        — chunk does not exist
//   - Internal        — unexpected I/O error
func (s *StorageServer) VerifyChunk(ctx context.Context, req *pb_storage.VerifyChunkRequest) (*pb_storage.VerifyChunkResponse, error) {
	chunkId := req.GetChunkId()
	s.logger.Info("VerifyChunk: called", slog.String("chunkId", chunkId))

	if chunkId == "" {
		s.logger.Error("VerifyChunk: empty chunk_id", dfserrors.ErrInvalidChunkId)
		return nil, status.Error(codes.InvalidArgument, "chunk_id must not be empty")
	}

	if !s.Store.Exists(chunkId) {
		s.logger.Error("VerifyChunk: chunk not found", dfserrors.ErrChunkNotFound,
			slog.String("chunkId", chunkId))
		return nil, status.Error(codes.NotFound, "chunk not found")
	}

	// Internal consistency check: recompute SHA-256 and compare to the stored
	// checksum index.
	verifyErr := s.Store.Verify(chunkId)
	if verifyErr != nil {
		if errors.Is(verifyErr, dfserrors.ErrVerifyFailed) {
			s.logger.Info("VerifyChunk: internal checksum mismatch — chunk corrupted",
				slog.String("chunkId", chunkId))
			return &pb_storage.VerifyChunkResponse{
				Response: &pb_storage.Response{Code: 200, Msg: "verification complete"},
				IsValid:  false,
			}, nil
		}
		s.logger.Error("VerifyChunk: store.Verify failed", verifyErr,
			slog.String("chunkId", chunkId))
		return nil, status.Error(codes.Internal, "verification failed")
	}

	// Caller-supplied checksum comparison (optional).
	if len(req.Checksum) > 0 {
		data, readErr := s.Store.Read(chunkId)
		if readErr != nil {
			s.logger.Error("VerifyChunk: read failed", readErr,
				slog.String("chunkId", chunkId))
			return nil, status.Error(codes.Internal, "failed to read chunk for verification")
		}

		computed := sha256.Sum256(data)
		if !bytes.Equal(computed[:], req.Checksum) {
			s.logger.Info("VerifyChunk: caller-supplied checksum mismatch",
				slog.String("chunkId", chunkId))
			return &pb_storage.VerifyChunkResponse{
				Response: &pb_storage.Response{Code: 200, Msg: "verification complete"},
				IsValid:  false,
			}, nil
		}
	}

	s.logger.Info("VerifyChunk: valid", slog.String("chunkId", chunkId))
	return &pb_storage.VerifyChunkResponse{
		Response: &pb_storage.Response{Code: 200, Msg: "verification complete"},
		IsValid:  true,
	}, nil
}
