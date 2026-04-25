package fsm

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Apply – top-level dispatch

func TestApply_InvalidJSON(t *testing.T) {
	m := newTestFSM(t)
	result := m.Apply(&raft.Log{Data: []byte("not-json")})
	require.Error(t, result.(error))
}

func TestApply_UnknownCommand(t *testing.T) {
	m := newTestFSM(t)
	log := makeRaftLog(t, MetadataCmdType(9999), struct{}{})
	result := m.Apply(log)
	require.Error(t, result.(error), "unknown command type must return an error")
}

func TestApply_RegisterNode_InvalidPayload(t *testing.T) {
	m := newTestFSM(t)
	// Build a MetadataCommand whose Payload is not valid JSON for CommandRegisterNode.
	cmd := MetadataCommand{Type: CmdRegisterNode, Payload: []byte("{")}
	data, err := json.Marshal(cmd)
	require.NoError(t, err)

	// Apply returns the inner unmarshal error for malformed payloads.
	result := m.Apply(&raft.Log{Data: data})
	require.Error(t, result.(error), "malformed inner payload must surface as an error")
}

// Apply – end-to-end via raft.Log (integration-style)

func TestApply_RegisterNode_EndToEnd(t *testing.T) {
	m := newTestFSM(t)
	now := time.Now()

	req := CommandRegisterNode{
		NodeID:     "e2e-node",
		Address:    "192.168.1.1:5000",
		FreeSpace:  8192,
		ChunkCount: 0,
		CreatedAt:  now,
	}
	log := makeRaftLog(t, CmdRegisterNode, req)
	result := m.Apply(log)
	assert.Nil(t, result, "successful Apply should return nil error")

	n, err := m.GetNode("e2e-node")
	require.NoError(t, err)
	assert.Equal(t, "192.168.1.1:5000", n.Address)
	assert.Equal(t, NodeStatusAlive, n.Status)
}
