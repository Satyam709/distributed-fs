package leaderclient

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/satyam709/distributed-fs/internal/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var _ grpc.ClientConnInterface = (*LeaderAwareClient)(nil)

// LeaderAwareClient is a gRPC connection wrapper that transparently handles
// metadata leader redirection, address rotation, and retry with exponential
// backoff for unary RPCs. It implements grpc.ClientConnInterface so it can
// be passed directly to generated gRPC stubs (e.g. pb_meta.NewMetadataServiceClient).
//
// Redirect flow for unary RPCs:
//  1. Call the RPC against the currently cached leader connection.
//  2. If the response carries status FailedPrecondition with trailer
//     "x-leader-grpc-addr", update the cache to point at the new leader
//     and retry immediately (no backoff — redirects are not failures).
//  3. On connectivity errors (Unavailable, DeadlineExceeded), rotate
//     through the seed address list with exponential backoff + jitter.
//  4. Non-retryable errors (NotFound, InvalidArgument, etc.) are returned
//     immediately without retry.
//
// NewStream is a pass-through to the current connection with no
// redirect/retry logic — currently no streaming RPCs are in use.
type LeaderAwareClient struct {
	cache       *LeaderCache
	retryPolicy retry.Policy
}

// New creates a LeaderAwareClient that attempts to connect to the first
// reachable address from seedAddrs. The retry policy controls how many
// times connectivity failures are retried and the backoff profile.
func New(ctx context.Context, seedAddrs []string, rp retry.Policy) (*LeaderAwareClient, error) {
	cache := NewLeaderCache(seedAddrs)

	for _, addr := range seedAddrs {
		if err := cache.Update(ctx, addr); err == nil {
			return &LeaderAwareClient{cache: cache, retryPolicy: rp}, nil
		}
		cache.AdvanceNextIdx()
	}

	return nil, fmt.Errorf("leaderclient: failed to connect to any seed address: %v", seedAddrs)
}

// Invoke performs a unary RPC with transparent redirect-following and retry.
// It satisfies grpc.ClientConnInterface so generated stubs use it natively.
func (c *LeaderAwareClient) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	delay := c.retryPolicy.Base

	for attempt := 0; attempt < c.retryPolicy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		conn := c.cache.Conn()
		if conn == nil {
			return fmt.Errorf("leaderclient: no metadata connection available")
		}

		var trailer metadata.MD
		callOpts := append(opts, grpc.Trailer(&trailer))

		err := conn.Invoke(ctx, method, args, reply, callOpts...)
		if err == nil {
			return nil
		}

		st, ok := status.FromError(err)

		if ok && st.Code() == codes.FailedPrecondition {
			if addrs := trailer.Get("x-leader-grpc-addr"); len(addrs) > 0 {
				if updateErr := c.cache.Update(ctx, addrs[0]); updateErr == nil {
					continue
				}
			}
			// Trailer not found — propagate to retry path below.
		}

		if ok && !isRetryableCode(st.Code()) {
			return err
		}

		if attempt < c.retryPolicy.MaxAttempts-1 {
			c.cache.AdvanceNextIdx()
			nextAddr := c.cache.NextAddr()
			if c.cache.Conn() == nil || nextAddr != "" {
				_ = c.cache.Update(ctx, nextAddr)
			}

			jitter := time.Duration(0)
			if half := int64(delay / 2); half > 0 {
				jitter = time.Duration(rand.Int64N(half))
			}
			sleep := min(delay+jitter, c.retryPolicy.Max)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
			}

			next := min(time.Duration(float64(delay)*c.retryPolicy.Multiplier), c.retryPolicy.Max)
			delay = next
		}
	}

	return fmt.Errorf("leaderclient: RPC failed after %d attempts", c.retryPolicy.MaxAttempts)
}

// NewStream creates a streaming RPC, delegating to the current connection.
func (c *LeaderAwareClient) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn := c.cache.Conn()
	if conn == nil {
		return nil, fmt.Errorf("leaderclient: no metadata connection available")
	}
	return conn.NewStream(ctx, desc, method, opts...)
}

func (c *LeaderAwareClient) Close() error {
	return c.cache.Close()
}

func isRetryableCode(c codes.Code) bool {
	switch c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}
