package service

import (
	"context"
	"fmt"

	"github.com/satyam709/distributed-fs/client/internal/metadataclient"
)

// ListService provides file listing operations.
type ListService struct {
	metadata metadataclient.Client
}

// NewListService creates a ListService.
func NewListService(meta metadataclient.Client) *ListService {
	return &ListService{metadata: meta}
}

// ListFiles returns all files from the metadata service, optionally filtered
// by a name prefix.
func (s *ListService) ListFiles(ctx context.Context, prefix string) ([]metadataclient.FileInfo, error) {
	files, err := s.metadata.ListFiles(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("list: failed to list files: %w", err)
	}
	return files, nil
}

// DeleteFile deletes a file from the metadata service.
func (s *ListService) DeleteFile(ctx context.Context, fileID string) error {
	if err := s.metadata.DeleteFile(ctx, fileID); err != nil {
		return fmt.Errorf("delete: failed to delete file: %w", err)
	}
	return nil
}
