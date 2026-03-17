package server

import (
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

// ReplicationServer implements the ReplicationServiceServer gRPC interface.
// It receives chunks from peer storage nodes via a bidirectional stream,
// persisting them through the same ChunkWriter pipeline used by PutChunk.
type ReplicationServer struct {
	pb_storage.UnimplementedReplicationServiceServer
	Store  store.Store
	logger *logging.CLogger
}

// NewReplicationServer creates a ReplicationServer. Returns an error if s is nil.
func NewReplicationServer(s store.Store, logger *logging.CLogger) (*ReplicationServer, error) {
	if s == nil {
		return nil, dfserrors.ErrInvalidChunkId
	}
	l := logging.NewCLogger()
	if logger != nil {
		l = logger
	}
	l = l.With(slog.String("component", "ReplicationServer"))
	return &ReplicationServer{Store: s, logger: l}, nil
}

// ReplicateChunk implements the internal P2P chunk-transfer RPC.
//
// Protocol (from design doc §8):
//   - Sender opens a bidirectional stream.
//   - First frame: IsFirst=true, carries ChunkId.
//   - Each frame: Data payload; receiver acks with Ok=true for flow control.
//   - Last frame: IsLast=true; receiver finalises the chunk, sends IsFinal response.
//
// Error codes:
//   - InvalidArgument — first frame missing or ChunkId empty / mismatch.
//   - Internal        — writer init, frame write, or finalise failure.
func (s *ReplicationServer) ReplicateChunk(
	stream grpc.BidiStreamingServer[pb_storage.ReplicateChunkRequest, pb_storage.ReplicateChunkResponse],
) error {
	var writer *chunk.ChunkWriter
	var registeredChunkId string

	s.logger.Info("ReplicateChunk: stream opened")

	for {
		frame, err := stream.Recv()

		if err == io.EOF {
			// Sender closed the send side.
			if writer == nil {
				s.logger.Error("ReplicateChunk: EOF without any frames", dfserrors.ErrInvalidChunkId)
				return status.Error(codes.InvalidArgument, "no frames received")
			}
			// Normal finish: the last frame sets IsLast; EOF just closes the send-half.
			s.logger.Info("ReplicateChunk: sender closed stream", slog.String("chunkId", registeredChunkId))
			return nil
		}

		if err != nil {
			s.logger.Error("ReplicateChunk: recv error", err)
			if writer != nil {
				writer.Abort()
			}
			return status.Error(codes.Internal, "stream recv error")
		}

		// First frame: validate and open writer.
		if frame.IsFirst {
			if frame.ChunkId == "" {
				return status.Error(codes.InvalidArgument, "chunk_id must not be empty on first frame")
			}
			registeredChunkId = frame.ChunkId
			s.logger.Debug("ReplicateChunk: first frame",
				slog.String("chunkId", registeredChunkId))

			w, wErr := chunk.NewChunkWriter(registeredChunkId, s.Store)
			if wErr != nil {
				s.logger.Error("ReplicateChunk: failed to open writer", wErr,
					slog.String("chunkId", registeredChunkId))
				return status.Error(codes.Internal, "failed to initialise chunk writer")
			}
			writer = w
		}

		if writer == nil {
			return status.Error(codes.InvalidArgument, "first frame must have is_first=true")
		}

		// Guard against chunk_id change mid-stream.
		if frame.ChunkId != "" && frame.ChunkId != registeredChunkId {
			s.logger.Error("ReplicateChunk: chunk_id mismatch", dfserrors.ErrInvalidChunkId,
				slog.String("registered", registeredChunkId),
				slog.String("received", frame.ChunkId),
			)
			writer.Abort()
			return status.Error(codes.InvalidArgument, "chunk_id mismatch mid-stream")
		}

		// Write the frame payload.
		if len(frame.Data) > 0 {
			if wErr := writer.Write(frame.Data); wErr != nil {
				s.logger.Error("ReplicateChunk: write failed", wErr,
					slog.String("chunkId", registeredChunkId))
				writer.Abort()
				return status.Error(codes.Internal, "failed to write frame")
			}
		}

		if frame.IsLast {
			// Finalise: ChunkWriter computes the checksum internally; we do not
			// receive an expected checksum from the sender — the sender verifies
			// its own side. We just commit and send the computed checksum back.
			finalChecksum := writer.ComputedChecksum()
			if fErr := writer.Finalize(finalChecksum); fErr != nil {
				s.logger.Error("ReplicateChunk: finalise failed", fErr,
					slog.String("chunkId", registeredChunkId))
				return status.Error(codes.Internal, "failed to finalise chunk")
			}
			s.logger.Info("ReplicateChunk: chunk stored",
				slog.String("chunkId", registeredChunkId))

			return stream.Send(&pb_storage.ReplicateChunkResponse{
				Ok:       true,
				IsFinal:  true,
				ChunkId:  registeredChunkId,
				Checksum: finalChecksum,
			})
		}

		// Per-frame ack for flow control.
		if sErr := stream.Send(&pb_storage.ReplicateChunkResponse{Ok: true}); sErr != nil {
			s.logger.Error("ReplicateChunk: send ack failed", sErr,
				slog.String("chunkId", registeredChunkId))
			writer.Abort()
			return status.Error(codes.Internal, "failed to send frame ack")
		}
	}
}
