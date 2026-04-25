package server

import (
	"encoding/json"
	"github.com/satyam709/distributed-fs/metadata/fsm"
)

// newCommand serialises the payload and wraps it in a MetadataCommand envelope.
// This is the single place where commands are built before being proposed to Raft.
func newCommand(cmdType fsm.MetadataCmdType, payload any) (fsm.MetadataCommand, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return fsm.MetadataCommand{}, err
	}
	return fsm.MetadataCommand{
		Type:    cmdType,
		Payload: data,
	}, nil
}
