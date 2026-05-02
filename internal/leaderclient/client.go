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

type LeaderAwareClient struct {
	cache       *LeaderCache
	retryPolicy retry.Policy
}

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

func (c *LeaderAwareClient) Invoke(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
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

		err := conn.Invoke(ctx, method, req, reply, callOpts...)
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
		}

		if ok && !isRetryableCode(st.Code()) {
			return err
		}

		if attempt < c.retryPolicy.MaxAttempts-1 {
			nextAddr := c.cache.NextAddr()
			_ = c.cache.Update(ctx, nextAddr)
			c.cache.AdvanceNextIdx()

			jitter := time.Duration(rand.Int64N(int64(delay / 2)))
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

func isRetryableCode(c codes.Code) bool {
	switch c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.FailedPrecondition:
		return true
	default:
		return false
	}
}

func (c *LeaderAwareClient) Close() error {
	return c.cache.Close()
}
