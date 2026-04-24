package metaclient

import (
	"context"
	"errors"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A sub interface of pb_meta.NewMetadataServiceClient, only comprises of operations allowed from storage node
type StorageMetadataClientInterface interface {
	RegisterNode(ctx context.Context, in *pb_meta.RegisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.RegisterNodeResponse, error)
	DeregisterNode(ctx context.Context, in *pb_meta.DeregisterNodeRequest, opts ...grpc.CallOption) (*pb_meta.DeregisterNodeResponse, error)
	Heartbeat(ctx context.Context, in *pb_meta.HeartbeatRequest, opts ...grpc.CallOption) (*pb_meta.HeartbeatResponse, error)
	ReportRepairResult(ctx context.Context, in *pb_meta.ReportRepairResultRequest, opts ...grpc.CallOption) (*pb_meta.ReportRepairResultResponse, error)
	CommitChunk(ctx context.Context, in *pb_meta.CommitChunkRequest, opts ...grpc.CallOption) (*pb_meta.CommitChunkResponse, error)
}

type StorageMetadataClient struct {
	StorageMetadataClientInterface
}

func NewMetadataClient(addr string) (*StorageMetadataClient, error) {
	grpcClient, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Join(errors.New("NewmetadataClient:"), err)
	}
	return &StorageMetadataClient{
		pb_meta.NewMetadataServiceClient(grpcClient),
	}, nil
}
