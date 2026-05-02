package storage

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
		"node_id": "storage-1",
		"grpc_addr": ":4100",
		"metadata_addrs": [":3100"],
		"data_dir": "/tmp/storage",
		"timeout": "30s",
		"heartbeat_interval": "5s",
		"replication_factor": 2
	}`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(jsonContent), 0o644)
	require.NoError(t, err)

	cfg, err := LoadFromJSON(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg, "cfg must not be nil")

	assert.Equal(t, "storage-1", cfg.NodeID)
	assert.Equal(t, ":4100", cfg.GRPCAddr)
	assert.Equal(t, []string{":3100"}, cfg.MetadataAddrs)
	assert.Equal(t, "/tmp/storage", cfg.DataDir)
	assert.Equal(t, 30*time.Second, cfg.Timeout)
	assert.Equal(t, 5*time.Second, cfg.HeartbeatInterval)
	assert.Equal(t, 2, cfg.ReplicationFactor)
}

func TestLoadFromJSON_FileNotFound(t *testing.T) {
	_, err := LoadFromJSON("/nonexistent/path/config.json")
	assert.Error(t, err)
}

func TestLoadFromJSON_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(`{invalid json}`), 0o644)
	require.NoError(t, err)

	_, err = LoadFromJSON(configPath)
	assert.Error(t, err)
}

func TestLoadFromJSON_AppliesDefaults(t *testing.T) {
	jsonContent := `{
		"metadata_addrs": [":3100"]
	}`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(jsonContent), 0o644)
	require.NoError(t, err)

	cfg, err := LoadFromJSON(configPath)
	require.NoError(t, err)
	require.NotNil(t, cfg, "cfg must not be nil")

	assert.Equal(t, DefaultStorageGRPCAddr, cfg.GRPCAddr)
	assert.Equal(t, DefaultStorageDataDir, cfg.DataDir)
	assert.Equal(t, DefaultStorageTimeout, cfg.Timeout)
	assert.Equal(t, DefaultStorageHeartbeatInterval, cfg.HeartbeatInterval)
	assert.Equal(t, DefaultStorageReplicationFactor, cfg.ReplicationFactor)
}

func TestDefaultConfig(t *testing.T) {
	defConfig := DefaultStorageNodeConfig()
	assert.Equal(t, defConfig.DataDir, DefaultStorageDataDir)
	assert.Equal(t, defConfig.GRPCAddr, DefaultStorageGRPCAddr)
	assert.Equal(t, defConfig.HeartbeatInterval, DefaultStorageHeartbeatInterval)
	assert.Equal(t, defConfig.MetadataAddrs, []string{DefaultStorageMetadataAddr})
	// assert.Equal(t, defConfig.NodeID, DefaultStorageDataDir)
	assert.Equal(t, defConfig.ReplicationFactor, DefaultStorageReplicationFactor)
	assert.Equal(t, defConfig.Timeout, DefaultStorageTimeout)
}

func TestLoadConfigDefaultFlow_DefaultsOnly(t *testing.T) {
	t.Setenv(EnvStorageJsonConfigPath, "")
	cfg, err := LoadConfigDefaultFlow()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, DefaultStorageGRPCAddr, cfg.GRPCAddr)
	assert.Equal(t, []string{DefaultStorageMetadataAddr}, cfg.MetadataAddrs)
	assert.Equal(t, DefaultStorageDataDir, cfg.DataDir)
	assert.Equal(t, DefaultStorageReplicationFactor, cfg.ReplicationFactor)
}

func TestLoadConfigDefaultFlow_WithEnvOverride(t *testing.T) {
	t.Setenv(EnvStorageJsonConfigPath, "")
	t.Setenv(EnvStorageGrpcAddr, ":5000")
	t.Setenv(EnvStorageDataDir, "/custom/data")
	t.Setenv(EnvStorageReplicationFactor, "5")

	cfg, err := LoadConfigDefaultFlow()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, ":5000", cfg.GRPCAddr)
	assert.Equal(t, "/custom/data", cfg.DataDir)
	assert.Equal(t, 5, cfg.ReplicationFactor)
}

func TestLoadConfigDefaultFlow_JSONOverridesDefaults(t *testing.T) {
	jsonContent := `{"grpc_addr": ":4500", "replication_factor": 4}`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(jsonContent), 0o644)
	require.NoError(t, err)

	t.Setenv(EnvStorageJsonConfigPath, configPath)

	cfg, err := LoadConfigDefaultFlow()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, ":4500", cfg.GRPCAddr)
	assert.Equal(t, 4, cfg.ReplicationFactor)
	assert.Equal(t, DefaultStorageDataDir, cfg.DataDir)
}

func TestLoadConfigDefaultFlow_EnvOverridesJSON(t *testing.T) {
	jsonContent := `{"grpc_addr": ":4500", "replication_factor": 4}`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configPath, []byte(jsonContent), 0o644)
	require.NoError(t, err)

	t.Setenv(EnvStorageJsonConfigPath, configPath)
	t.Setenv(EnvStorageGrpcAddr, ":6000")

	cfg, err := LoadConfigDefaultFlow()
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, ":6000", cfg.GRPCAddr, "env should override JSON")
	assert.Equal(t, 4, cfg.ReplicationFactor)
}

func TestLoadConfigDefaultFlow_InvalidJSONPath(t *testing.T) {
	t.Setenv(EnvStorageJsonConfigPath, "/nonexistent/path/config.json")

	_, err := LoadConfigDefaultFlow()
	assert.Error(t, err)
}

func TestLoadConfigDefaultFlow_InvalidEnvDuration(t *testing.T) {
	t.Setenv(EnvStorageJsonConfigPath, "")
	t.Setenv(EnvStorageTimeout, "invalid-duration")

	_, err := LoadConfigDefaultFlow()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid env var STORAGE_TIMEOUT")
}

func TestApplyEnv_InvalidReplicationFactor(t *testing.T) {
	t.Setenv(EnvStorageReplicationFactor, "not-a-number")

	cfg := DefaultStorageNodeConfig()
	err := cfg.ApplyEnv()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid env var STORAGE_REPLICATION_FACTOR")
}
