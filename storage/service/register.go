package service

import (
	"context"
	"log/slog"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/metaclient"
)

type StorageNodeRegisterer struct {
	logger     *logging.CLogger
	nodeinfo   NodeInfo
	metaClient metaclient.StorageMetadataClientInterface
}

func NewStorageNodeRegisterer(ni NodeInfo, mc metaclient.StorageMetadataClientInterface) *StorageNodeRegisterer {
	return &StorageNodeRegisterer{
		logger:     logging.NewCLogger().With("component", "NodeRegisterer"),
		nodeinfo:   ni,
		metaClient: mc,
	}
}

// registerWithMetadata registers this storage node with the metadata cluster.
// It scans the local store for existing chunks and reports them as part of
// the registration. Called during startup. If registration fails, the node
// will retry via heartbeat.
func (s *StorageNodeRegisterer) RegisterWithMetadata(ctx context.Context) error {
	chunkIDs, err := s.nodeinfo.GetChunkList()
	if err != nil {
		return err
	}

	s.logger.Info("StorageNode: registering with metadata",
		slog.String("nodeID", s.nodeinfo.GetNodeID()),
		slog.String("addr", s.nodeinfo.GetGrpcAddr()),
		slog.Int("chunkCount", len(chunkIDs)))

	resp, err := s.metaClient.RegisterNode(ctx, &pb_meta.RegisterNodeRequest{
		NodeId:    s.nodeinfo.GetNodeID(),
		Address:   s.nodeinfo.GetGrpcAddr(),
		FreeSpace: int64(s.nodeinfo.GetFreeSpace()),
		ChunkIds:  chunkIDs,
	})
	if err != nil {
		return err
	}

	s.logger.Info("StorageNode: registered with metadata",
		slog.String("confirmedNodeID", resp.NodeId))
	return nil
}

// deregisterFromMetadata marks this node as leaving the cluster.
// Called during graceful shutdown. Marks the node as draining so no new
// chunks are placed on it, and triggers repair for its existing chunks.
func (s *StorageNodeRegisterer) DeregisterFromMetadata(ctx context.Context) error {
	s.logger.Info("StorageNode: deregistering from metadata",
		slog.String("nodeID", s.nodeinfo.GetNodeID()))

	_, err := s.metaClient.DeregisterNode(ctx, &pb_meta.DeregisterNodeRequest{
		NodeId: s.nodeinfo.GetNodeID(),
	})
	if err != nil {
		s.logger.Warn("StorageNode: failed to deregister",
			slog.String("err", err.Error()))
		return err
	}

	s.logger.Info("StorageNode: deregistered from metadata")
	return nil
}
