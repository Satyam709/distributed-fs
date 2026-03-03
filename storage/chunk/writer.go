package chunk

import (
	"encoding/hex"
	"errors"
	"hash"
	"os"

	"github.com/satyam709/distributed-fs/storage/store"
)

var (
	OperationAborted    error = errors.New("op already aborted")
	WriterClosed        error = errors.New("writer closed")
	ErrChecksumMismatch error = errors.New("checksum mismatch")
)

type ChunkWriter struct {
	filepath string
	file     *os.File
	chunkId  string
	store    store.Store
	isDone   bool
	hasher   hash.Hash
	written  int64
}

func NewChunkWriter(chuckId string, store store.Store) (*ChunkWriter, error) {
	cw := &ChunkWriter{
		chunkId:  chuckId,
		store:    store,
		filepath: store.TempDir(chuckId),
	}
	f, err := os.Create(cw.filepath)
	if err != nil {
		return nil, err
	}
	cw.file = f
	return cw, nil
}

func (cw *ChunkWriter) Write(data []byte) error {
	if cw.isDone {
		return WriterClosed
	}
	n, err := cw.file.Write(data)
	cw.hasher.Write(data[:n])
	cw.written += int64(n)
	err = cw.file.Sync()
	return err
}

// called on last frame — verifies checksum and atomically commits
func (cw *ChunkWriter) Finalize(expectedChecksum string) (err error) {
	computed := hex.EncodeToString(cw.hasher.Sum(nil))
	if computed != expectedChecksum {
		return ErrChecksumMismatch
	}

	// if there is err make sure to abort the op
	defer func() {
		if err != nil {
			cw.Abort()
		}
	}()

	cw.file.Sync()
	cw.file.Close()
	cw.isDone = true

	// atomic rename
	finalPath, err := cw.store.PathForChunk(cw.chunkId)
	if err != nil {
		return
	}
	err = cw.store.Rename(cw.filepath, finalPath)
	return err
}

// called when stream dies mid-transfer — clean up partial file
func (cw *ChunkWriter) Abort() {
	cw.file.Close()
	os.Remove(cw.filepath)
	cw.isDone = true
}
