package server

import (
	"context"
	"io"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"

	"github.com/satyam709/distributed-fs/storage/chunk"
	"github.com/satyam709/distributed-fs/storage/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StorageServer is the implementation for the StorageServiceServer
type StorageServer struct {
	pb_storage.UnimplementedStorageServiceServer
	Store    store.Store
}

func (s StorageServer) PutChunk(stream grpc.ClientStreamingServer[pb_storage.PutChunkRequest, pb_storage.PutChunkResponse]) error {
	var writer *chunk.ChunkWriter
	for {
		data, err := stream.Recv()
		if err == io.EOF || data.IsLast {
			if writer == nil {
				return status.Error(codes.Internal, "failed to putchunk")
			}
			// finalize the chunk
			finlizeErr := writer.Finalize(string(data.Checksum))
			if finlizeErr != nil {
				return status.Error(codes.Internal, "failed to putchunk")
			}
			stream.SendAndClose(&pb_storage.PutChunkResponse{Response: &pb_storage.Response{
				Code: 200,
				Msg:  "Chunk Stored",
			},
				ChunkId:  data.ChunkId,
				Checksum: string(data.Checksum),
			})
		}
		if err != nil {
			return status.Error(codes.Internal, "stream broken")
		}
		// consume data

		// write to a temp PATH
		if writer == nil {
			writer, err = chunk.NewChunkWriter(data.ChunkId, s.Store)
			if err != nil {
				return status.Error(codes.Internal, "failed to putchunk")
			}
		}
	}
}
func (s StorageServer) GetChunk(*pb_storage.GetChunkRequest, grpc.ServerStreamingServer[pb_storage.GetChunkResponse]) error {
	return status.Error(codes.Unimplemented, "method GetChunk not implemented")
}
func (s StorageServer) DeleteChunk(*pb_storage.DeleteChunkRequest, grpc.ServerStreamingServer[pb_storage.DeleteChunkResponse]) error {
	return status.Error(codes.Unimplemented, "method DeleteChunk not implemented")
}
func (s StorageServer) VerifyChunk(context.Context, *pb_storage.VerifyChunkRequest) (*pb_storage.VerifyChunkResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method VerifyChunk not implemented")
}
