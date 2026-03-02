package server

import (
	"context"
	"io"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"

	"github.com/satyam709/distributed-fs/storage/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StorageServer is the implementation for the StorageServiceServer
type StorageServer struct {
	pb_storage.UnimplementedStorageServiceServer
	store store.ChecksumIndexStore[string]
}

func (s StorageServer) PutChunk(req grpc.ClientStreamingServer[pb_storage.PutChunkRequest, pb_storage.PutChunkResponse]) error {
	for {
		_, err := req.Recv()
		if err == io.EOF {

		}
		if err != nil {

		}

	}
	return status.Error(codes.Unimplemented, "method PutChunk not implemented")
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
