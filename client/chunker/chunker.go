package chunker

import (
    "math"
    "os"
    "github.com/satyam709/distributed-fs/internal/checksum" // Use your internal helper
    "github.com/google/uuid"
)

type ChunkDescriptor struct {
    ChunkID    string
    FileID     string
    ChunkIndex int
    Offset     int64
    Size       int64
}

func GenerateDescriptors(filePath string, chunkSize int64) ([]ChunkDescriptor, error) {
    fileInfo, err := os.Stat(filePath)
    if err != nil {
        return nil, err
    }

    totalSize := fileInfo.Size()
    fileID := uuid.New().String() // Section 2: UUID once per file
    numChunks := int(math.Ceil(float64(totalSize) / float64(chunkSize)))

    var descriptors []ChunkDescriptor
    for i := 0; i < numChunks; i++ {
        offset := int64(i) * chunkSize
        size := chunkSize
        if offset+chunkSize > totalSize {
            size = totalSize - offset
        }

        // Section 2: SHA256(file_id + index)
        chunkID := checksum.ComputeChunkID(fileID, i) 

        descriptors = append(descriptors, ChunkDescriptor{
            ChunkID:    chunkID,
            FileID:     fileID,
            ChunkIndex: i,
            Offset:     offset,
            Size:       size,
        })
    }
    return descriptors, nil
}