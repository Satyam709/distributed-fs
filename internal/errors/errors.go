// Package errors provides shared sentinel errors used across the distributed-fs
// storage subsystem. Package-specific errors (e.g. BoltDB operational errors)
// stay in their own packages; only errors that cross package boundaries live here.
package errors

import "errors"

var (
	// ErrInvalidChunkId is returned when a chunk identifier is empty or too
	// short to derive a valid shard path.
	ErrInvalidChunkId = errors.New("chunk-id is not valid")

	// ErrChunkNotFound is returned when a requested chunk does not exist in
	// the store.
	ErrChunkNotFound = errors.New("chunk not found")

	// ErrChecksumMismatch is returned when the computed checksum of a written
	// chunk does not match the expected value supplied by the caller.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	// ErrInsufficientSpace is returned when the store does not have enough
	// free capacity to accept a write.
	ErrInsufficientSpace = errors.New("insufficient space")

	// ErrFailedToDelete is returned when an on-disk removal of a chunk file
	// fails. It is typically wrapped with the underlying OS error via fmt.Errorf.
	ErrFailedToDelete = errors.New("chunk deletion failed")

	// ErrVerifyFailed is returned by Verify when the stored checksum does not
	// match the recomputed hash of the chunk data.
	ErrVerifyFailed = errors.New("chunk mismatch checksum verification failed")
)
