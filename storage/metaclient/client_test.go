package metaclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMetadataClient(t *testing.T) {
	client, err := NewMetadataClient("localhost:12345")
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.NotNil(t, client.StorageMetadataClientInterface)
}

func TestNewMetadataClient_InvalidAddr(t *testing.T) {
	client, err := NewMetadataClient("invalid-address-!!!")
	assert.NoError(t, err)
	assert.NotNil(t, client)
}