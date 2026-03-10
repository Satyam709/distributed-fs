package manifest

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type UploadManifest struct {
	FileID      string          `json:"file_id"`
	Filename    string          `json:"filename"`
	TotalSize   int64           `json:"total_size"`
	ChunkStatus map[string]bool `json:"chunk_status"` // ChunkID -> IsDone
}

func Save(manifestDir string, m *UploadManifest) error {
	path := filepath.Join(manifestDir, m.FileID+".json")
	data, _ := json.MarshalIndent(m, "", "  ")
	return os.WriteFile(path, data, 0644)
}
