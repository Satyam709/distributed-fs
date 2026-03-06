package replication_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/satyam709/distributed-fs/storage/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryPolicy_SucceedsOnFirstAttempt(t *testing.T) {
	p := replication.DefaultRetryPolicy()
	calls := 0
	err := p.Do(context.Background(), func() error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

func TestRetryPolicy_RetriesOnError(t *testing.T) {
	p := replication.RetryPolicy{
		MaxAttempts: 3,
		Base:        1 * time.Millisecond,
		Max:         5 * time.Millisecond,
		Multiplier:  2.0,
	}
	calls := 0
	sentinel := errors.New("transient")
	err := p.Do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return sentinel
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRetryPolicy_ExhaustsAttempts(t *testing.T) {
	p := replication.RetryPolicy{
		MaxAttempts: 3,
		Base:        1 * time.Millisecond,
		Max:         5 * time.Millisecond,
		Multiplier:  2.0,
	}
	sentinel := errors.New("always fails")
	calls := 0
	err := p.Do(context.Background(), func() error {
		calls++
		return sentinel
	})
	require.Error(t, err)
	assert.Equal(t, 3, calls)
	assert.ErrorIs(t, err, sentinel)
}

func TestRetryPolicy_HonoursContextCancel(t *testing.T) {
	p := replication.RetryPolicy{
		MaxAttempts: 10,
		Base:        50 * time.Millisecond,
		Max:         200 * time.Millisecond,
		Multiplier:  2.0,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	calls := 0
	err := p.Do(ctx, func() error {
		calls++
		return errors.New("fail")
	})
	require.Error(t, err)
	// Should not complete all 10 attempts.
	assert.Less(t, calls, 10)
}
