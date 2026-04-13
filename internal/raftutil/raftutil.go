package raftutil

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

func IsLeader(r *raft.Raft) bool {
	return r.State() == raft.Leader
}

func LeaderAddress(r *raft.Raft) string {
	return string(r.Leader())
}

func WaitForLeader(r *raft.Raft, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr := r.Leader(); addr != "" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for raft leader")
}
