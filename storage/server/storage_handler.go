package server

import (
	"context"
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

// PutChunk receives a client-streaming RPC that delivers chunk frames and
// atomically commits the chunk once all frames have been received.
func (s *StorageServer) PutChunk(stream grpc.ClientStreamingServer[pb_storage.PutChunkRequest, pb_storage.PutChunkResponse]) error {
	var writer *chunk.ChunkWriter

	s.logger.Info("PutChunk: stream opened")
	var lastFrame *pb_storage.PutChunkRequest

	for {
		data, err := stream.Recv()

		// Stream closed by the client (EOF)
		// finalize  if stream is closed with a isLast
		if err == io.EOF {
			if writer == nil {
				s.logger.Error("PutChunk: EOF before any frame received", dfserrors.ErrInvalidChunkId)
				return status.Error(codes.InvalidArgument, "no data received")
			}

			// stream is broken
			if lastFrame == nil || !lastFrame.IsLast {
				s.logger.Error("PutChunk: invalid stream closure", dfserrors.ErrInvalidChunkId)
				writer.Abort()
				return status.Error(codes.InvalidArgument, "no data received")
			}

			s.logger.Info("PutChunk: finalising chunk", slog.String("chunkId", lastFrame.ChunkId))

			if finalizeErr := writer.Finalize(lastFrame.Checksum); finalizeErr != nil {
				s.logger.Error("PutChunk: finalize failed", finalizeErr,
					slog.String("chunkId", lastFrame.ChunkId))
				return status.Error(codes.Internal, "failed to finalize chunk")
			}

			s.logger.Info("PutChunk: chunk stored successfully", slog.String("chunkId", lastFrame.ChunkId))
			return stream.SendAndClose(&pb_storage.PutChunkResponse{
				Response: &pb_storage.Response{
					Code: 200,
					Msg:  "Chunk Stored",
				},
				ChunkId:  lastFrame.ChunkId,
				Checksum: lastFrame.Checksum,
			})
		}

		if err != nil {
			s.logger.Error("PutChunk: stream recv error", err)
			if writer != nil {
				writer.Abort()
			}
			return status.Error(codes.Internal, "stream error")
		}

		// First frame — create the writer.
		if writer == nil {
			s.logger.Debug("PutChunk: first frame, creating writer",
				slog.String("chunkId", data.ChunkId))

			writer, err = chunk.NewChunkWriter(data.ChunkId, s.Store)

			if err != nil {
				s.logger.Error("PutChunk: failed to create writer", err,
					slog.String("chunkId", data.ChunkId))
				return status.Error(codes.Internal, "failed to initialise chunk writer")
			}
		}

		// Write this frame's payload.
		s.logger.Debug("PutChunk: writing frame",
			slog.String("chunkId", data.ChunkId),
			slog.Int("frameBytes", len(data.Data)),
		)

		if writeErr := writer.Write(data.Data); writeErr != nil {

			s.logger.Error("PutChunk: frame write failed", writeErr,
				slog.String("chunkId", data.ChunkId))

			writer.Abort()
			return status.Error(codes.Internal, "failed to write chunk frame")
		}

		lastFrame = data
	}
}

func (s *StorageServer) GetChunk(_ *pb_storage.GetChunkRequest, _ grpc.ServerStreamingServer[pb_storage.GetChunkResponse]) error {
	s.logger.Info("GetChunk: called (unimplemented)")
	return status.Error(codes.Unimplemented, "method GetChunk not implemented")
}

func (s *StorageServer) DeleteChunk(_ *pb_storage.DeleteChunkRequest, _ grpc.ServerStreamingServer[pb_storage.DeleteChunkResponse]) error {
	s.logger.Info("DeleteChunk: called (unimplemented)")
	return status.Error(codes.Unimplemented, "method DeleteChunk not implemented")
}

func (s *StorageServer) VerifyChunk(_ context.Context, _ *pb_storage.VerifyChunkRequest) (*pb_storage.VerifyChunkResponse, error) {
	s.logger.Info("VerifyChunk: called (unimplemented)")
	return nil, status.Error(codes.Unimplemented, "method VerifyChunk not implemented")
}
