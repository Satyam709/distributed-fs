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
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, c.dialOpts...)
	newConn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("leaderclient: failed to dial %s: %w", addr, err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentAddr == addr && c.conn != nil {
		_ = newConn.Close()
		return nil
	}

	oldConn := c.conn
	c.conn = newConn
	c.currentAddr = addr

	if oldConn != nil {
		_ = oldConn.Close()
	}
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
