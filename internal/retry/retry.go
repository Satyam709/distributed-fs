package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

type Policy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
	Multiplier  float64
}

func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 5,
		Base:        100 * time.Millisecond,
		Max:         5 * time.Second,
		Multiplier:  2.0,
	}
}

func (p Policy) Do(ctx context.Context, fn func() error) error {
	var lastErr error
	delay := p.Base

	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if attempt == p.MaxAttempts-1 {
			break
		}

		jitter := time.Duration(rand.Int64N(int64(delay / 2)))
		sleep := min(delay+jitter, p.Max)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		next := min(time.Duration(float64(delay)*p.Multiplier), p.Max)
		delay = next
	}

	return errors.Join(errors.New("all retry attempts exhausted"), lastErr)
}
