package storage

import (
	"errors"
	"time"
)

type StorageNodeConfig struct {
	// Port to start server on, e.g. ":4000"
	Port string
	// Timeout for connections (0 = no timeout)
	Timeout time.Duration

	// address of a node in Metadata cluster
	// probably have to change this
	MetadataAddr string
}

func (c StorageNodeConfig) Validate() error {
	if c.Port == "" {
		return errors.New("StorageNodeConfig: Port must not be empty")
	}
	return nil
}
