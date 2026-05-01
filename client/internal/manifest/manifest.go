package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// UploadManifest records the state of an in-progress upload for crash recovery.
type UploadManifest struct {
	FileID      string          `json:"file_id"`
	Filename    string          `json:"filename"`
	TotalSize   int64           `json:"total_size"`
	ChunkSize   int64           `json:"chunk_size"`
	ChunkStatus map[string]bool `json:"chunk_status"` // ChunkID → IsDone
}

// Manager handles manifest lifecycle — create, load, update, delete.
type Manager struct {
	manifestDir string
	mu          sync.Mutex
}

// NewManager creates a Manager that stores manifests in the given directory.
func NewManager(dir string) *Manager {
	return &Manager{manifestDir: dir}
}

// manifestPath returns the file path for a given fileID's manifest.
func (m *Manager) manifestPath(fileID string) string {
	return filepath.Join(m.manifestDir, fileID+".json")
}

// Create writes a new manifest to disk. The manifest directory is created if needed.
func (m *Manager) Create(manifest *UploadManifest) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := os.MkdirAll(m.manifestDir, 0755); err != nil {
		return fmt.Errorf("manifest: failed to create directory %s: %w", m.manifestDir, err)
	}
	return m.writeLocked(manifest)
}

// Load reads an existing manifest from disk. Returns an error if it doesn't exist.
func (m *Manager) Load(fileID string) (*UploadManifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := m.manifestPath(fileID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to read %s: %w", path, err)
	}

	var manifest UploadManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("manifest: failed to parse %s: %w", path, err)
	}
	return &manifest, nil
}

// Exists checks whether a manifest file exists for the given fileID.
func (m *Manager) Exists(fileID string) bool {
	path := m.manifestPath(fileID)
	_, err := os.Stat(path)
	return err == nil
}

// MarkChunkDone atomically marks a chunk as completed and persists the manifest.
func (m *Manager) MarkChunkDone(fileID, chunkID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := m.manifestPath(fileID)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("manifest: failed to read for update %s: %w", path, err)
	}

	var manifest UploadManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("manifest: failed to parse for update %s: %w", path, err)
	}

	manifest.ChunkStatus[chunkID] = true
	return m.writeLocked(&manifest)
}

// Delete removes the manifest file for a given fileID.
func (m *Manager) Delete(fileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	path := m.manifestPath(fileID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("manifest: failed to delete %s: %w", path, err)
	}
	return nil
}

// GetPendingChunks returns the chunk IDs that have not yet been marked done.
func (m *Manager) GetPendingChunks(fileID string) ([]string, error) {
	manifest, err := m.Load(fileID)
	if err != nil {
		return nil, err
	}

	var pending []string
	for chunkID, done := range manifest.ChunkStatus {
		if !done {
			pending = append(pending, chunkID)
		}
	}
	return pending, nil
}

// writeLocked persists the manifest to disk. Caller must hold m.mu.
func (m *Manager) writeLocked(manifest *UploadManifest) error {
	path := m.manifestPath(manifest.FileID)
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("manifest: failed to marshal: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("manifest: failed to write %s: %w", path, err)
	}
	return nil
}
