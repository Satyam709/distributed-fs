package storageclient

import (
	"context"
	"fmt"
	"io"
	"sync"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultFrameSize = 32 * 1024 // 32KB

// GRPCClient implements Client using gRPC with connection pooling.
type GRPCClient struct {
	mu        sync.Mutex
	conns     map[string]*grpc.ClientConn // addr → connection
	frameSize int
}

// NewGRPCClient creates a new storage client with the given frame size.
// If frameSize <= 0, defaults to 32KB.
func NewGRPCClient(frameSize int) *GRPCClient {
	if frameSize <= 0 {
		frameSize = defaultFrameSize
	}
	return &GRPCClient{
		conns:     make(map[string]*grpc.ClientConn),
		frameSize: frameSize,
	}
}

// getOrDial returns a cached connection or dials a new one.
func (g *GRPCClient) getOrDial(addr string) (pb_storage.StorageServiceClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if conn, ok := g.conns[addr]; ok {
		return pb_storage.NewStorageServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("storageclient: failed to connect to %s: %w", addr, err)
	}
	g.conns[addr] = conn
	return pb_storage.NewStorageServiceClient(conn), nil
}

func (g *GRPCClient) PutChunk(ctx context.Context, addr string, chunkID, fileID string, chunkIndex int, data []byte, checksum []byte, replicateTo []string) error {
	client, err := g.getOrDial(addr)
	if err != nil {
		return err
	}

	stream, err := client.PutChunk(ctx)
	if err != nil {
		return fmt.Errorf("storageclient: PutChunk stream open failed: %w", err)
	}

	// Build replicate_to node info list
	var replicaNodes []*pb_storage.NodeInfo
	for _, rAddr := range replicateTo {
		replicaNodes = append(replicaNodes, &pb_storage.NodeInfo{Address: rAddr})
	}

	// Framing: split data into frameSize frames
	for i := 0; i < len(data); i += g.frameSize {
		end := i + g.frameSize
		if end > len(data) {
			end = len(data)
		}
		isLast := end == len(data)

		req := &pb_storage.PutChunkRequest{
			ChunkId:     chunkID,
			FileId:      fileID,
			ReplicateTo: replicaNodes,
			Data:        data[i:end],
			Checksum:    checksum,
			IsLast:      isLast,
		}

		if err := stream.Send(req); err != nil {
			return fmt.Errorf("storageclient: PutChunk send frame failed: %w", err)
		}
	}

	// Handle empty data edge case
	if len(data) == 0 {
		req := &pb_storage.PutChunkRequest{
			ChunkId:     chunkID,
			FileId:      fileID,
			ReplicateTo: replicaNodes,
			Data:        nil,
			Checksum:    checksum,
			IsLast:      true,
		}
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("storageclient: PutChunk send empty frame failed: %w", err)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("storageclient: PutChunk close failed: %w", err)
	}
	return nil
}

func (g *GRPCClient) GetChunk(ctx context.Context, addr string, chunkID string) ([]byte, error) {
	client, err := g.getOrDial(addr)
	if err != nil {
		return nil, err
	}

	stream, err := client.GetChunk(ctx, &pb_storage.GetChunkRequest{ChunkId: chunkID})
	if err != nil {
		return nil, fmt.Errorf("storageclient: GetChunk stream open failed: %w", err)
	}

	var chunkData []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("storageclient: GetChunk recv failed: %w", err)
		}
		chunkData = append(chunkData, resp.GetData()...)
	}

	return chunkData, nil
}

// Close shuts down all pooled connections.
func (g *GRPCClient) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	var firstErr error
	for addr, conn := range g.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("storageclient: failed to close connection to %s: %w", addr, err)
		}
	}
	g.conns = make(map[string]*grpc.ClientConn)
	return firstErr
}
