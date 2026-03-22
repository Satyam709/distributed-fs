package server

import (
	"context"
	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
)

func(h *MetadataServiceHandler) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse , error){
	if(!h.isLeader()){
		return nil, h.leaderRedirect();
	}
	return nil,nil;
}

func(h *MetadataServiceHandler) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error){
	if(!h.isLeader()){
		return nil, h.leaderRedirect();
	}
	return nil,nil;
}

func (h *MetadataServiceHandler) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
    if(!h.isLeader()) {
        return nil, h.leaderRedirect();
    }
    return nil, nil;
}

func (h *MetadataServiceHandler) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
    if(!h.isLeader()){
        return nil, h.leaderRedirect();
    }
    return nil, nil;
}

