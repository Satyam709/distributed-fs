package metadataclient

import (
	"context"
	"fmt"
	"sync"
)

// MockClient is an in-memory implementation of Client for testing.
// It supports configurable responses, error injection, and call tracking.
type MockClient struct {
	mu sync.Mutex

	// --- Configurable state ---

	// Files stored by fileID
	Files map[string]*FileInfo
	// Chunks stored by chunkID
	Chunks map[string]ChunkInfo
	// Placements returned by CreateFile, keyed by fileName
	PlacementsByFile map[string][]Placement
	// FileID counter for CreateFile
	nextFileID int

	// --- Error injection ---

	// If set, CreateFile returns this error
	CreateFileErr error
	// If set, CommitChunk returns this error
	CommitChunkErr error
	// If set, GetFile returns this error
	GetFileErr error
	// If set, ListFiles returns this error
	ListFilesErr error
	// If set, DeleteFile returns this error
	DeleteFileErr error
	// If set, GetChunkLocations returns this error
	GetChunkLocationsErr error

	// --- Call tracking ---
	CreateFileCalls      []createFileCall
	CommitChunkCalls     []commitChunkCall
	GetFileCalls         []string
	DeleteFileCalls      []string
}

type createFileCall struct {
	FileName  string
	FileSize  int64
	ChunkSize int64
	ChunkIDs  []string
}

type commitChunkCall struct {
	ChunkID        string
	FileID         string
	ConfirmedNodes []string
	Checksum       string
}

// NewMockClient creates a MockClient with empty state.
func NewMockClient() *MockClient {
	return &MockClient{
		Files:            make(map[string]*FileInfo),
		Chunks:           make(map[string]ChunkInfo),
		PlacementsByFile: make(map[string][]Placement),
	}
}

func (m *MockClient) CreateFile(_ context.Context, fileName string, fileSize int64, chunkSize int64, chunkIDs []string) (string, []Placement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CreateFileCalls = append(m.CreateFileCalls, createFileCall{
		FileName: fileName, FileSize: fileSize, ChunkSize: chunkSize, ChunkIDs: chunkIDs,
	})

	if m.CreateFileErr != nil {
		return "", nil, m.CreateFileErr
	}

	m.nextFileID++
	fileID := fmt.Sprintf("file-%03d", m.nextFileID)

	m.Files[fileID] = &FileInfo{
		FileID:    fileID,
		FileName:  fileName,
		FileSize:  fileSize,
		ChunkSize: chunkSize,
		ChunkIDs:  chunkIDs,
		Status:    "creating",
	}

	placements := m.PlacementsByFile[fileName]
	if placements == nil {
		// Auto-generate default placements — one primary per chunk
		for _, cid := range chunkIDs {
			placements = append(placements, Placement{
				ChunkID:  cid,
				Primary:  "storage-node-1:4000",
				Replicas: []string{"storage-node-2:4000", "storage-node-3:4000"},
			})
		}
	}

	return fileID, placements, nil
}

func (m *MockClient) CommitChunk(_ context.Context, chunkID, fileID string, confirmedNodes []string, checksum string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CommitChunkCalls = append(m.CommitChunkCalls, commitChunkCall{
		ChunkID: chunkID, FileID: fileID, ConfirmedNodes: confirmedNodes, Checksum: checksum,
	})

	if m.CommitChunkErr != nil {
		return m.CommitChunkErr
	}
	return nil
}

func (m *MockClient) GetFile(_ context.Context, fileID string) (*FileInfo, []ChunkInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.GetFileCalls = append(m.GetFileCalls, fileID)

	if m.GetFileErr != nil {
		return nil, nil, m.GetFileErr
	}

	fi, ok := m.Files[fileID]
	if !ok {
		return nil, nil, fmt.Errorf("mock: file %q not found", fileID)
	}

	var chunks []ChunkInfo
	for _, cid := range fi.ChunkIDs {
		if c, exists := m.Chunks[cid]; exists {
			chunks = append(chunks, c)
		}
	}
	return fi, chunks, nil
}

func (m *MockClient) GetFileByName(_ context.Context, fileName string) (*FileInfo, []ChunkInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.GetFileErr != nil {
		return nil, nil, m.GetFileErr
	}

	for _, fi := range m.Files {
		if fi.FileName == fileName {
			var chunks []ChunkInfo
			for _, cid := range fi.ChunkIDs {
				if c, exists := m.Chunks[cid]; exists {
					chunks = append(chunks, c)
				}
			}
			return fi, chunks, nil
		}
	}
	return nil, nil, fmt.Errorf("mock: file %q not found", fileName)
}

func (m *MockClient) ListFiles(_ context.Context, prefix string) ([]FileInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ListFilesErr != nil {
		return nil, m.ListFilesErr
	}

	var result []FileInfo
	for _, fi := range m.Files {
		if prefix == "" || len(fi.FileName) >= len(prefix) && fi.FileName[:len(prefix)] == prefix {
			result = append(result, *fi)
		}
	}
	return result, nil
}

func (m *MockClient) DeleteFile(_ context.Context, fileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeleteFileCalls = append(m.DeleteFileCalls, fileID)

	if m.DeleteFileErr != nil {
		return m.DeleteFileErr
	}

	delete(m.Files, fileID)
	return nil
}

func (m *MockClient) GetChunkLocations(_ context.Context, chunkID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.GetChunkLocationsErr != nil {
		return nil, m.GetChunkLocationsErr
	}

	if c, ok := m.Chunks[chunkID]; ok {
		return c.Replicas, nil
	}
	return nil, fmt.Errorf("mock: chunk %q not found", chunkID)
}
