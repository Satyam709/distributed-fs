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
}

// Validate returns an error if any required field is missing.
func (c StorageNodeConfig) Validate() error {
	if c.Port == "" {
		return errors.New("StorageNodeConfig: Port must not be empty")
	}
	return nil
}
