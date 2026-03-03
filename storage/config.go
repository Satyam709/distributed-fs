package storage

import "time"

type StorageNodeConfig struct {
	// Port to start server on
	Port    string
	// Timeout
	Timeout time.Duration
}
