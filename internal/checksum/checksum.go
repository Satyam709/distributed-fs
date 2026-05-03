// internal/checksum/checksum.go
package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Compute returns the raw 32-byte SHA-256 hash of the provided byte slice.
// Used across the system for chunk data checksum verification.
func Compute(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// ComputeChunkID generates a unique hex-encoded SHA256 ID for a specific chunk.
// It uses the formula: SHA256(file_id + chunk_index).
func ComputeChunkID(fileID string, index int) string {
	input := fmt.Sprintf("%s%d", fileID, index)
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}
