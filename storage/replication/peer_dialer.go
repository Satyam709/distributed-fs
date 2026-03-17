// Package replication implements P2P chunk replication for the storage node.
//
// PeerDialer manages a pool of gRPC connections to peer storage nodes.
// Connections are cached, health-checked on reuse, and guarded by an RWMutex
// to allow high read concurrency with low-frequency writes (new peers).
package replication

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/satyam709/distributed-fs/internal/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// defaultKeepalive matches the design doc: detect dead peers in ~13 s.
var defaultKeepalive = keepalive.ClientParameters{
	Time:                5 * time.Second,
	Timeout:             8 * time.Second,
	PermitWithoutStream: true,
}

// PeerDialer is a thread-safe gRPC connection pool.
//
// Usage:
//
//	d := NewPeerDialer()
//	defer d.CloseAll()
//	conn, err := d.Get("10.0.0.1:4001")
type PeerDialer struct {
	mu     sync.RWMutex
	conns  map[string]*grpc.ClientConn
	opts   []grpc.DialOption
	logger *logging.CLogger
}

// NewPeerDialer creates a PeerDialer with keepalive enabled.
// Extra dial options (e.g. TLS credentials) can be passed via opts.
func NewPeerDialer(opts ...grpc.DialOption) *PeerDialer {
	base := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(defaultKeepalive),
	}
	l := logging.NewCLogger().With(slog.String("component", "PeerDialer"))
	return &PeerDialer{
		conns:  make(map[string]*grpc.ClientConn),
		opts:   append(base, opts...),
		logger: l,
	}
}

// Get returns a healthy gRPC connection to address, dialing if necessary.
//
// Double-check pattern:
//  1. Read lock — return cached conn if healthy.
//  2. Write lock — check again, then dial.
func (d *PeerDialer) Get(address string) (*grpc.ClientConn, error) {
	// Fast path: cached connection is still healthy.
	d.mu.RLock()
	if conn, ok := d.conns[address]; ok && d.isHealthy(conn) {
		d.mu.RUnlock()
		return conn, nil
	}
	d.mu.RUnlock()

	// Slow path: acquire write lock, double-check, then dial.
	d.mu.Lock()
	defer d.mu.Unlock()

	if conn, ok := d.conns[address]; ok && d.isHealthy(conn) {
		return conn, nil
	}

	// Close the stale connection before replacing it (best-effort).
	if old, ok := d.conns[address]; ok {
		_ = old.Close()
		delete(d.conns, address)
	}

	d.logger.Info("PeerDialer: dialing peer", slog.String("address", address))
	conn, err := grpc.NewClient(address, d.opts...)
	if err != nil {
		return nil, fmt.Errorf("PeerDialer.Get(%q): dial failed: %w", address, err)
	}
	d.conns[address] = conn
	return conn, nil
}

// Remove evicts the connection for address from the pool and closes it.
// Safe to call when the peer is known to be dead.
func (d *PeerDialer) Remove(address string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if conn, ok := d.conns[address]; ok {
		d.logger.Info("PeerDialer: removing peer", slog.String("address", address))
		_ = conn.Close()
		delete(d.conns, address)
	}
}

// CloseAll closes every cached connection. Call during node shutdown.
func (d *PeerDialer) CloseAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.logger.Info("PeerDialer: closing all connections", slog.Int("count", len(d.conns)))
	for addr, conn := range d.conns {
		_ = conn.Close()
		delete(d.conns, addr)
	}
}

// isHealthy returns true if the connection has not entered a terminal state.
// Must be called under mu (read or write lock held by caller).
func (d *PeerDialer) isHealthy(conn *grpc.ClientConn) bool {
	s := conn.GetState()
	return s != connectivity.TransientFailure && s != connectivity.Shutdown
}
