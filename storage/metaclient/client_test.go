package metaclient

import (
	"testing"
	"time"

	"github.com/satyam709/distributed-fs/internal/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetadataClient(t *testing.T) {
	rp := retry.Policy{MaxAttempts: 1, Base: 1 * time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2.0}
	client, err := NewMetadataClient([]string{"localhost:12345"}, rp, 10*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, client)
}

func TestNewMetadataClient_InvalidAddr(t *testing.T) {
	rp := retry.Policy{MaxAttempts: 1, Base: 1 * time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2.0}
	client, err := NewMetadataClient([]string{"invalid-address-!!!"}, rp, 10*time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, client)
}
