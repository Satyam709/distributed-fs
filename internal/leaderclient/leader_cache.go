package leaderclient

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LeaderCache struct {
	mu          sync.RWMutex
	currentAddr string
	seedAddrs   []string
	nextIdx     int
	conn        *grpc.ClientConn
	dialOpts    []grpc.DialOption
}

func NewLeaderCache(seedAddrs []string) *LeaderCache {
	return &LeaderCache{seedAddrs: seedAddrs}
}

func (c *LeaderCache) Conn() *grpc.ClientConn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

func (c *LeaderCache) Update(ctx context.Context, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentAddr == addr && c.conn != nil {
		return nil
	}

	if c.conn != nil {
		_ = c.conn.Close()
	}

	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, c.dialOpts...)
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("leaderclient: failed to dial %s: %w", addr, err)
	}

	c.conn = conn
	c.currentAddr = addr
	return nil
}

func (c *LeaderCache) NextAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.seedAddrs) == 0 {
		return c.currentAddr
	}

	return c.seedAddrs[c.nextIdx%len(c.seedAddrs)]
}

func (c *LeaderCache) AdvanceNextIdx() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextIdx++
}

func (c *LeaderCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
