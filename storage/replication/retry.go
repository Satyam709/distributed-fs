package replication

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// RetryPolicy defines exponential-backoff-with-jitter retry behaviour.
//
// Design doc spec (§ ReplicationManager):
//   - 3 attempts, 500 ms base, 10 s max, 2× multiplier
//   - Random jitter on each sleep — prevents thundering herd
//   - Respects context cancellation
type RetryPolicy struct {
	MaxAttempts int           // total number of tries (including the first)
	Base        time.Duration // initial delay before the second attempt
	Max         time.Duration // upper bound on the delay
	Multiplier  float64       // growth factor per attempt
}

// DefaultRetryPolicy returns the policy matching the design doc.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Base:        500 * time.Millisecond,
		Max:         10 * time.Second,
		Multiplier:  2.0,
	}
}

// Do calls fn up to MaxAttempts times, sleeping between attempts with
// exponential backoff and uniform jitter in [0, delay/2).
//
// Returns nil on first success.
// Returns the last error if all attempts are exhausted.
// Returns ctx.Err() immediately if the context is cancelled.
func (r RetryPolicy) Do(ctx context.Context, fn func() error) error {
	var lastErr error
	delay := r.Base

	for attempt := 0; attempt < r.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt == r.MaxAttempts-1 {
			break // no sleep after the final attempt
		}

		// Apply jitter: sleep for delay + rand[0, delay/2).
		jitter := time.Duration(rand.Int64N(int64(delay / 2)))
		sleep := min(delay+jitter, r.Max)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		// Grow delay for the next round, capped at Max.
		next := min(time.Duration(float64(delay)*r.Multiplier), r.Max)
		delay = next
	}

	return errors.Join(errors.New("all retry attempts exhausted"), lastErr)
}
