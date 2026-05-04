package replication

import (
	"time"

	"github.com/satyam709/distributed-fs/internal/retry"
)

type RetryPolicy = retry.Policy

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Base:        500 * time.Millisecond,
		Max:         10 * time.Second,
		Multiplier:  2.0,
	}
}
