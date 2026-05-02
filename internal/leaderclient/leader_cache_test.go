package leaderclient

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeaderCache_Update_NewAddr(t *testing.T) {
	c := NewLeaderCache([]string{"localhost:1"})
	err := c.Update(context.Background(), "localhost:2")
	require.NoError(t, err)

	conn := c.Conn()
	require.NotNil(t, conn)

	_ = c.Close()
}

func TestLeaderCache_Update_SameAddr_Noop(t *testing.T) {
	c := NewLeaderCache([]string{"localhost:1"})
	err := c.Update(context.Background(), "localhost:1")
	require.NoError(t, err)

	conn1 := c.Conn()
	require.NotNil(t, conn1)

	err = c.Update(context.Background(), "localhost:1")
	require.NoError(t, err)

	conn2 := c.Conn()
	assert.Same(t, conn1, conn2, "same addr should not create new connection")

	_ = c.Close()
}

func TestLeaderCache_Update_SwitchesConnection(t *testing.T) {
	c := NewLeaderCache(nil)
	err := c.Update(context.Background(), "localhost:1")
	require.NoError(t, err)
	conn1 := c.Conn()
	require.NotNil(t, conn1)

	err = c.Update(context.Background(), "localhost:2")
	require.NoError(t, err)
	conn2 := c.Conn()
	require.NotNil(t, conn2)

	assert.NotSame(t, conn1, conn2, "different addr must create new connection")

	_ = c.Close()
}

func TestLeaderCache_NextAddr_RoundRobin(t *testing.T) {
	c := NewLeaderCache([]string{"a:1", "b:2", "c:3"})

	assert.Equal(t, "a:1", c.NextAddr())
	c.AdvanceNextIdx()

	assert.Equal(t, "b:2", c.NextAddr())
	c.AdvanceNextIdx()

	assert.Equal(t, "c:3", c.NextAddr())
	c.AdvanceNextIdx()

	// wraps around
	assert.Equal(t, "a:1", c.NextAddr())
}

func TestLeaderCache_NextAddr_EmptySeeds(t *testing.T) {
	c := NewLeaderCache(nil)
	assert.Equal(t, "", c.NextAddr())
}

func TestLeaderCache_ConcurrentAccess(t *testing.T) {
	c := NewLeaderCache([]string{"localhost:1"})
	err := c.Update(context.Background(), "localhost:1")
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Conn()
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.NextAddr()
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.AdvanceNextIdx()
		}()
	}
	wg.Wait()
}

func TestLeaderCache_Conn_NilBeforeUpdate(t *testing.T) {
	c := NewLeaderCache([]string{"localhost:1"})
	assert.Nil(t, c.Conn())
}

func TestLeaderCache_Close(t *testing.T) {
	c := NewLeaderCache([]string{"localhost:1"})
	err := c.Update(context.Background(), "localhost:1")
	require.NoError(t, err)
	err = c.Close()
	assert.NoError(t, err)
}
