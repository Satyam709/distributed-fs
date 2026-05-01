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
	_, err := h.deps.FSM.GetFile(req.FileId)
	if err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "file with ID '%s' already exists", req.FileId)
	}

	// Validate chunk_size: must be within [64KB, 64MB]
	const (
		minChunkSize = 64 * 1024        // 64 KB
		maxChunkSize = 64 * 1024 * 1024 // 64 MB
	)
	if req.ChunkSize <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "chunk_size must be positive, got %d", req.ChunkSize)
	}
	if req.ChunkSize < minChunkSize {
		return nil, status.Errorf(codes.InvalidArgument, "chunk_size %d is below minimum %d (64 KB)", req.ChunkSize, minChunkSize)
	}
	if req.ChunkSize > maxChunkSize {
		return nil, status.Errorf(codes.InvalidArgument, "chunk_size %d exceeds maximum %d (64 MB)", req.ChunkSize, maxChunkSize)
	}

	// get the placements for chunks of file
	var aliveNodes []fsm.NodeEntry
	if aliveNodes, err = h.deps.FSM.GetLiveNodes(); err != nil {
		return nil, status.Errorf(codes.Internal, "error getting live nodes: %v", err)
	}
	var chPlacements []*pb.ChunkPlacement
	rf := h.deps.ReplicationFactor
	for _, v := range req.ChunkIds {
		outNodes, err := h.deps.TargetPlacement.SelectNodes(aliveNodes, v, rf)
		if err != nil || len(outNodes) == 0 {
			return nil, status.Errorf(codes.Internal, "error getting placement for chunk %s: %v", v, err)
		}

		transform := func(nodes ...fsm.NodeEntry) []*pb.NodeInfo {
			out := []*pb.NodeInfo{}
			for _, v := range nodes {
				out = append(out, &pb.NodeInfo{
					NodeId:    v.NodeID,
					Address:   v.Address,
					FreeSpace: int64(v.FreeSpace),
				})
			}
			return out
		}

		out := transform(outNodes...)

		chPlacements = append(chPlacements, &pb.ChunkPlacement{
			ChunkId:  v,
			Primary:  out[0],
			Replicas: out[1:],
		})
	}

	// chunks placement done -> create file
	cmd, err := newCommand(fsm.CmdCreateFile, fsm.CommandCreateFile{
		FileID:    req.FileId,
		FileName:  req.FileName,
		FileSize:  uint64(req.FileSize),
		ChunkSize: uint64(req.ChunkSize),
		ChunkIDs:  req.ChunkIds,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error creating command: %v", err)
	}

	if err := fsm.Propose(h.deps.Raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing command: %v", err)
	}

	// TODO: should there be a dedicated handler to give the placement options for a chunk
	// currently chunk replica in fsm is populated by storage nodes at CommitChunk
	return &pb.CreateFileResponse{FileId: req.FileId, Placements: chPlacements}, nil
}

func (h *MetadataServiceHandler) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}

	// direct fsm read - no raft involvement since this is a
	// read-only operation on the leader's state machine

	file, err := h.deps.FSM.GetFile(req.FileId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "file not found: %s", req.FileId)
	}
	// get all chunks for this file in order
	chunks, err := h.deps.FSM.GetFileChunks(req.FileId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error fetching file chunks: %v", err)
	}

	// for each chunk, get its live nodes location
	pbChunks := make([]*pb.ChunkInfo, 0, len(chunks))

	for _, ck := range chunks {
		pbChunks = append(pbChunks, &pb.ChunkInfo{
			ChunkId:    ck.ChunkID,
			FileId:     ck.FileID,
			ChunkIndex: int32(ck.ChunkIndex),
			Size:       ck.Size,
			Checksum:   ck.Checksum,
			Replicas:   ck.Replicas,
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
	_, err := h.deps.FSM.GetFile(req.FileId)
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

	if err := fsm.Propose(h.deps.Raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing delete command: %v", err)
	}
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (h *MetadataServiceHandler) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	// direct fsm read - rsults already sorted by fileName
	// files, err := h.fsm.ListFiles(req.Prefix)
	files, err := h.deps.FSM.ListFiles("") // ignore prefix filter for now
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
	if err := fsm.Propose(h.deps.Raft, cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "error proposing commit command: %v", err)
	}
	return &pb.CommitFileResponse{Success: true}, nil

}
