package server

import (
	"context"
	"time"

	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/metadata/fsm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *MetadataServiceHandler) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	// check duplicate filename
	_, err := h.fsm.GetFileByName(req.FileName)
	if err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "file with name '%s' already exists", req.FileName)
	}

	//generate server-side file ID
	fileID := generateID()
	cmd, err := newCommand(fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID:    fileID,
		FileName:  req.FileName,
		FileSize:  uint64(req.FileSize),
		ChunkIDs:  req.ChunkIds,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating command: %v", err)
	}

	if err := fsm.Propose(h.raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing command: %v", err)
	}

	return &pb.CreateFileResponse{FileId: fileID}, nil
}

func (h *MetadataServiceHandler) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}

	// direct fsm read - no raft involvement since this is a
	// read-only operation on the leader's state machine

	file, err := h.fsm.GetFile(req.FileId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "file not found: %s", req.FileId)
	}
	// get all chunks for this file in order
	chunks, err := h.fsm.GetFileChunks(req.FileId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error fetching file chunks: %v", err)
	}

	// for each chunk, get its live nodes location
	pbChunks := make([]*pb.ChunkInfo, 0, len(chunks))

	for _, ck := range chunks {
		locations, err := h.fsm.GetChunkLocations(ck.ChunkID)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error fetching chunk locations: %v", err)
		}
		// collect node IDs of live replicas
		nodeIDs := make([]string, 0, len(locations))
		for _, n := range locations {
			nodeIDs = append(nodeIDs, n.NodeID)
		}

		pbChunks = append(pbChunks, &pb.ChunkInfo{
			ChunkId:    ck.ChunkID,
			FileId:     ck.FileID,
			ChunkIndex: int32(ck.ChunkIndex),
			Size:       ck.Size,
			Checksum:   ck.Checksum,
			Replicas:   nodeIDs,
			Status:     string(ck.Status),
		})
	}

	return &pb.GetFileResponse{
		File: &pb.FileInfo{
			FileId:    file.FileID,
			FileName:  file.Filename,
			FileSize:  int64(file.FileSize),
			ChunkSize: int64(file.ChunkSize),
			ChunkIds:  file.ChunkIDs,
			Status:    string(file.Status),
			CreatedAt: file.CreatedAt.Unix(),
			UpdatedAt: file.UpdatedAt.Unix(),
		},
		Chunks: pbChunks,
	}, nil
}

func (h *MetadataServiceHandler) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	// verofy file exosts before proposing - fall fast with clear error
	_, err := h.fsm.GetFile(req.FileId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "file not found: %s", req.FileId)
	}

	// propose cmdDeleteFile through raft
	cmd, err := newCommand(fsm.CmdDeleteFile, fsm.CommandDeleteFile{
		FileID: req.FileId,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating delete command: %v", err)
	}

	if err := fsm.Propose(h.raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing delete command: %v", err)
	}
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (h *MetadataServiceHandler) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	// direct fsm read - rsults already sorted by fileName
	files, err := h.fsm.ListFiles(req.Prefix)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error listing files: %v", err)
	}

	pbFiles := make([]*pb.FileInfo, 0, len(files))
	for _, f := range files {
		pbFiles = append(pbFiles, &pb.FileInfo{
			FileId:    f.FileID,
			FileName:  f.Filename,
			FileSize:  int64(f.FileSize),
			ChunkSize: int64(f.ChunkSize),
			ChunkIds:  f.ChunkIDs,
			Status:    string(f.Status),
			CreatedAt: f.CreatedAt.Unix(),
			UpdatedAt: f.UpdatedAt.Unix(),
		})
	}
	return &pb.ListFilesResponse{Files: pbFiles}, nil
}

func (h *MetadataServiceHandler) CommitFile(ctx context.Context, req *pb.CommitFileRequest) (*pb.CommitFileResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	cmd, err := newCommand(fsm.CmdCommitFile, fsm.CommandCommitFile{
		FileID:   req.FileId,
		FileSize: uint64(req.FileSize),
		Checksum: req.Checksum,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating commit command: %v", err)
	}
	if err := fsm.Propose(h.raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing commit command: %v", err)
	}
	return &pb.CommitFileResponse{Success: true}, nil

}
