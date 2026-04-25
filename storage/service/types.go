package service

import "context"

type NodeInfo interface {
	GetFreeSpace() uint64
	GetNodeID() string
	GetChunkCount() uint32
	GetChunkList() ([]string, error)
	GetGrpcAddr() string
}

type NodeRegisterer interface {
	RegisterWithMetadata(ctx context.Context) error
	DeregisterFromMetadata(ctx context.Context) error
}
