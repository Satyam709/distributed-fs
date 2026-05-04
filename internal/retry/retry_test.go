package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetry_SuccessFirstAttempt(t *testing.T) {
	p := DefaultPolicy()
	calls := 0
	err := p.Do(context.Background(), func() error {
		calls++
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestRetry_SuccessAfterFailures(t *testing.T) {
	p := Policy{MaxAttempts: 5, Base: 1 * time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2.0}
	calls := 0
	err := p.Do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRetry_Exhausted(t *testing.T) {
	p := Policy{MaxAttempts: 3, Base: 1 * time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2.0}
	calls := 0
	err := p.Do(context.Background(), func() error {
		calls++
		return errors.New("persistent")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "all retry attempts exhausted")
	assert.Contains(t, err.Error(), "persistent")
	assert.Equal(t, 3, calls)
}

func TestRetry_ContextCancelled(t *testing.T) {
	p := DefaultPolicy()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.Do(ctx, func() error {
		return errors.New("should not execute")
	})
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRetry_ContextCancelledDuringBackoff(t *testing.T) {
	p := Policy{MaxAttempts: 5, Base: 100 * time.Millisecond, Max: 1 * time.Second, Multiplier: 2.0}
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	calls := 0
	err := p.Do(ctx, func() error {
		calls++
		return errors.New("fail")
	})
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}
