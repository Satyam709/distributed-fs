// Package replication — ReplicationManager: P2P fanout and repair worker pool.
package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/chunk"
	"github.com/satyam709/distributed-fs/storage/store"
)

const (
	maxConcurrentStreams = 8   // semaphore capacity — prevents disk/network saturation
	repairQueueCap       = 256 // buffered repair job channel
	numRepairWorkers     = 4
	fanoutTimeout        = 30 * time.Second
)

// RepairJob is a single repair instruction piggybacked on a heartbeat response.
type RepairJob struct {
	JobID   string
	ChunkID string
	Source  string // address of source node (self, normally)
	Target  string // address of replica to repair
}

type Replicator interface {
	EnqueueRepair(job RepairJob) bool
}

// ReplicationManager fans out chunk data to replica nodes after a primary write
// and drains a background repair queue populated by metadata heartbeat responses.
type ReplicationManager struct {
	dialer  *PeerDialer
	store   store.Store
	policy  RetryPolicy
	sem     chan struct{} // limits concurrent outbound streams
	repairQ chan RepairJob
	logger  *logging.CLogger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewReplicationManager creates a manager. Call Start() to launch repair workers.
func NewReplicationManager(dialer *PeerDialer, s store.Store) *ReplicationManager {
	l := logging.NewCLogger().With(slog.String("component", "ReplicationManager"))

	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicationManager{
		dialer:  dialer,
		store:   s,
		policy:  DefaultRetryPolicy(),
		sem:     make(chan struct{}, maxConcurrentStreams),
		repairQ: make(chan RepairJob, repairQueueCap),
		logger:  l,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start launches the repair worker pool. Safe to call once.
func (m *ReplicationManager) Start() {
	for i := range numRepairWorkers {
		m.wg.Add(1)
		go m.worker(i)
	}
	m.logger.Info("ReplicationManager: started", slog.Int("workers", numRepairWorkers))
}

// Stop signals workers to stop and waits for them to drain.
func (m *ReplicationManager) Stop() {
	m.logger.Info("ReplicationManager: stopping")
	m.cancel()
	m.wg.Wait()
	m.logger.Info("ReplicationManager: stopped")
}

// EnqueueRepair queues a repair job non-blocking.
// Returns true if the job was accepted, false if the queue is full.
func (m *ReplicationManager) EnqueueRepair(job RepairJob) bool {
	select {
	case m.repairQ <- job:
		m.logger.Info("ReplicationManager: repair enqueued",
			slog.String("chunkId", job.ChunkID),
			slog.String("target", job.Target),
		)
		return true
	default:
		m.logger.Info("ReplicationManager: repair queue full, dropping job",
			slog.String("chunkId", job.ChunkID))
		return false
	}
}

// ReplicateToNodes fans out chunkId to every address in targets concurrently.
//
//   - async=false: blocks until all goroutines finish (or fanoutTimeout elapses),
//     evaluates quorum (majority), returns error if quorum not met.
//   - async=true: fires goroutines and returns immediately (fire-and-forget).
func (m *ReplicationManager) ReplicateToNodes(ctx context.Context, chunkId string, targets []string, async bool) error {
	if len(targets) == 0 {
		return nil
	}

	type result struct {
		addr string
		err  error
	}
	results := make(chan result, len(targets))

	for _, addr := range targets {
		addr := addr
		go func() {
			err := m.policy.Do(ctx, func() error {
				return m.replicateToSingleNode(ctx, chunkId, addr)
			})
			results <- result{addr: addr, err: err}
		}()
	}

	if async {
		return nil
	}

	// Collect with timeout.
	timer := time.NewTimer(fanoutTimeout)
	defer timer.Stop()

	var successCount int
	var errs []error
	for range len(targets) {
		select {
		case r := <-results:
			if r.err != nil {
				m.logger.Info("ReplicationManager: replica failed",
					slog.String("chunkId", chunkId),
					slog.String("target", r.addr),
					slog.String("error", r.err.Error()),
				)
				errs = append(errs, fmt.Errorf("%s: %w", r.addr, r.err))
			} else {
				m.logger.Info("ReplicationManager: replica succeeded",
					slog.String("chunkId", chunkId),
					slog.String("target", r.addr),
				)
				successCount++
			}
		case <-timer.C:
			return fmt.Errorf("replication fan-out timed out after %s: %d/%d succeeded",
				fanoutTimeout, successCount, len(targets))
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	quorum := len(targets)/2 + 1
	if successCount < quorum {
		return fmt.Errorf("replication quorum not met: %d/%d succeeded (need %d): %w",
			successCount, len(targets), quorum, errors.Join(errs...))
	}
	return nil
}

// replicateToSingleNode opens a ReplicateChunk bidi stream to address and
// pipes the chunk stored in m.store to the receiving node.
func (m *ReplicationManager) replicateToSingleNode(ctx context.Context, chunkId, address string) error {
	// Acquire semaphore slot.
	select {
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.sem }()

	conn, err := m.dialer.Get(address)
	if err != nil {
		return fmt.Errorf("dial %q: %w", address, err)
	}

	client := pb_storage.NewReplicationServiceClient(conn)
	stream, err := client.ReplicateChunk(ctx)
	if err != nil {
		return fmt.Errorf("open ReplicateChunk stream to %q: %w", address, err)
	}

	reader, err := chunk.NewChunkReader(chunkId, m.store, 0 /* default 32 KiB */)
	if err != nil {
		return fmt.Errorf("open ChunkReader for %q: %w", chunkId, err)
	}
	defer reader.Close()

	isFirst := true
	for {
		frame, nextErr := reader.Next()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			return fmt.Errorf("read frame for %q: %w", chunkId, nextErr)
		}

		req := &pb_storage.ReplicateChunkRequest{
			ChunkId: chunkId,
			Data:    frame.Data,
			IsFirst: isFirst,
			IsLast:  frame.IsLast,
		}
		isFirst = false

		if sendErr := stream.Send(req); sendErr != nil {
			return fmt.Errorf("send frame for %q to %q: %w", chunkId, address, sendErr)
		}

		// Recv per-frame ack for flow control.
		ack, recvErr := stream.Recv()
		if recvErr != nil {
			m.logger.Info("recv err", "err", recvErr.Error())

			return fmt.Errorf("recv ack from %q: %w", address, recvErr)
		}
		if !ack.Ok {
			return fmt.Errorf("negative ack from %q for chunk %q", address, chunkId)
		}

		if frame.IsLast {
			break
		}
	}

	// Close send side and read final response.
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close send to %q: %w", address, err)
	}

	// we dont expect a legit response upon send stream closure
	// only error is useful to check
	_, err = stream.Recv()
	if err != nil && err != io.EOF {
		return fmt.Errorf("recv final response from %q: %w", address, err)
	}

	m.logger.Info("replicateToSingleNode: success",
		slog.String("chunkId", chunkId),
		slog.String("address", address),
	)
	return nil
}

// worker drains the repair queue and calls replicateToSingleNode with retry.
func (m *ReplicationManager) worker(id int) {
	defer m.wg.Done()
	m.logger.Info("repair worker started", slog.Int("worker", id))

	for {
		select {
		case <-m.ctx.Done():
			m.logger.Info("repair worker exiting", slog.Int("worker", id))
			return
		case job, ok := <-m.repairQ:
			if !ok {
				return
			}
			m.logger.Info("repair worker picked job",
				slog.Int("worker", id),
				slog.String("chunkId", job.ChunkID),
				slog.String("target", job.Target),
			)
			err := m.policy.Do(m.ctx, func() error {
				return m.replicateToSingleNode(m.ctx, job.ChunkID, job.Target)
			})
			if err != nil {
				m.logger.Info("repair worker: job failed after retries",
					slog.Int("worker", id),
					slog.String("chunkId", job.ChunkID),
					slog.String("target", job.Target),
					slog.String("error", err.Error()),
				)
				// TODO: call MetadataService.ReportRepairFailure once the metadata client exists.
			} else {
				m.logger.Info("repair worker: job succeeded",
					slog.Int("worker", id),
					slog.String("chunkId", job.ChunkID),
				)
				// TODO: call MetadataService.CommitChunk once the metadata client exists.
			}
		}
	}
}
