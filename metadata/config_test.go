package metadata

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromJSON(t *testing.T) {
	jsonContent := `{
		"node_id": "node-1",
		"grpc_addr": ":4001",
		"raft_addr": "127.0.0.1:5001",
		"raft_dir": "/tmp/raft",
		"peer_addrs": {"node-2": "127.0.0.1:5002"},
		"bootstrap": true,
		"replication_factor": 5,
		"heartbeat_timeout": "2s",
		"election_timeout": "10s"
	}`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(jsonContent), 0644)
	require.NoError(t, err)

	cfg, err := LoadFromJSON(configPath)
	require.NoError(t, err)

	assert.Equal(t, "node-1", cfg.NodeID)
	assert.Equal(t, ":4001", cfg.GRPCAddr)
	assert.Equal(t, "127.0.0.1:5001", cfg.RaftAddr)
	assert.Equal(t, "/tmp/raft", cfg.RaftDir)
	assert.True(t, cfg.Bootstrap)
	assert.Equal(t, 5, cfg.ReplicationFactor)
	assert.Equal(t, 2*time.Second, cfg.HeartbeatTimeout)
	assert.Equal(t, 10*time.Second, cfg.ElectionTimeout)

	assert.Equal(t, "127.0.0.1:5002", cfg.PeerAddrs["node-2"])
}

func TestLoadFromJSON_FileNotFound(t *testing.T) {
	_, err := LoadFromJSON("/nonexistent/path/config.json")
	assert.Error(t, err)
}

func TestLoadFromJSON_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(`{invalid json}`), 0644)
	require.NoError(t, err)

	_, err = LoadFromJSON(configPath)
	assert.Error(t, err)
}

func TestLoadFromJSON_AppliesDefaults(t *testing.T) {
	jsonContent := `{
		"node_id": "node-1",
		"grpc_addr": ":4001",
		"raft_addr": "127.0.0.1:5001",
		"raft_dir": "/tmp/raft"
	}`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(jsonContent), 0644)
	require.NoError(t, err)

	cfg, err := LoadFromJSON(configPath)
	require.NoError(t, err)

	assert.Equal(t, DefaultReplicationFactor, cfg.ReplicationFactor)
	assert.Equal(t, DefaultSuspectTimeout, cfg.SuspectTimeout)
	assert.Equal(t, DefaultDeadTimeout, cfg.DeadTimeout)
	assert.Equal(t, DefaultHeartbeatTimeout, cfg.HeartbeatTimeout)
}
