package storageclient

import (
	"context"
	"fmt"
	"sync"
)

// MockClient is an in-memory implementation of Client for testing.
// It stores chunk data and supports failure injection.
type MockClient struct {
	mu sync.Mutex

	// Stored chunks: key = "addr:chunkID", value = data
	Chunks map[string][]byte

	// Track all PutChunk calls for verification
	PutCalls []PutCall

	// Track all GetChunk calls for verification
	GetCalls []GetCall

	// Error injection: if a chunkID is in this set, PutChunk returns an error
	PutFailChunks map[string]int // chunkID → remaining failures (decremented each call)

	// Error injection: if a chunkID is in this set, GetChunk returns an error
	GetFailChunks map[string]int // chunkID → remaining failures

	// Error injection: if an addr is in this set, all operations to it fail
	FailAddrs map[string]bool
}

// PutCall records a single PutChunk invocation.
type PutCall struct {
	Addr        string
	ChunkID     string
	FileID      string
	ChunkIndex  int
	DataLen     int
	Checksum    string
	ReplicateTo []string
}

// GetCall records a single GetChunk invocation.
type GetCall struct {
	Addr    string
	ChunkID string
}

// NewMockClient creates a MockClient with empty state.
func NewMockClient() *MockClient {
	return &MockClient{
		Chunks:        make(map[string][]byte),
		PutFailChunks: make(map[string]int),
		GetFailChunks: make(map[string]int),
		FailAddrs:     make(map[string]bool),
	}
}

func (m *MockClient) PutChunk(_ context.Context, addr string, chunkID, fileID string, chunkIndex int, data []byte, checksum string, replicateTo []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PutCalls = append(m.PutCalls, PutCall{
		Addr:        addr,
		ChunkID:     chunkID,
		FileID:      fileID,
		ChunkIndex:  chunkIndex,
		DataLen:     len(data),
		Checksum:    checksum,
		ReplicateTo: replicateTo,
	})

	if m.FailAddrs[addr] {
		return fmt.Errorf("mock: storage node %s is unreachable", addr)
	}

	if remaining, ok := m.PutFailChunks[chunkID]; ok && remaining > 0 {
		m.PutFailChunks[chunkID] = remaining - 1
		return fmt.Errorf("mock: PutChunk failed for chunk %s (injected failure)", chunkID)
	}

	// Store the chunk data
	key := addr + ":" + chunkID
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	m.Chunks[key] = dataCopy

	return nil
}

func (m *MockClient) GetChunk(_ context.Context, addr string, chunkID string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.GetCalls = append(m.GetCalls, GetCall{
		Addr:    addr,
		ChunkID: chunkID,
	})

	if m.FailAddrs[addr] {
		return nil, fmt.Errorf("mock: storage node %s is unreachable", addr)
	}

	if remaining, ok := m.GetFailChunks[chunkID]; ok && remaining > 0 {
		m.GetFailChunks[chunkID] = remaining - 1
		return nil, fmt.Errorf("mock: GetChunk failed for chunk %s (injected failure)", chunkID)
	}

	key := addr + ":" + chunkID
	data, ok := m.Chunks[key]
	if !ok {
		return nil, fmt.Errorf("mock: chunk %s not found on %s", chunkID, addr)
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return dataCopy, nil
}

func (m *MockClient) Close() error {
	return nil
}
