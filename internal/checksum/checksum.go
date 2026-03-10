// internal/checksum/checksum.go
package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Compute returns the hex-encoded SHA256 hash of the provided byte slice.
// Used by ParallelUploader to checksum chunk data before upload.
func Compute(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ComputeChunkID generates a unique hex-encoded SHA256 ID for a specific chunk.
// It uses the formula: SHA256(file_id + chunk_index).
func ComputeChunkID(fileID string, index int) string {
	// Concatenate fileID and index as a string to create a unique input
	input := fmt.Sprintf("%s%d", fileID, index)
	return Compute([]byte(input))
}
