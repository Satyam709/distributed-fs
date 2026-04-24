package heartbeat

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb_meta "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
	"github.com/satyam709/distributed-fs/internal/logging"
	"github.com/satyam709/distributed-fs/storage/metaclient"
	"github.com/satyam709/distributed-fs/storage/replication"
)

const (
	REQUEST_TIMEOUT = 10
)

type NodeInfo interface {
	GetFreeSpace() uint64
	GetNodeID() string
	GetChunkCount() uint32
}

type HeartbeatSender struct {
	gap        time.Duration
	client     metaclient.StorageMetadataClientInterface
	nodeInfo   NodeInfo
	replicator replication.Replicator
	logger     *logging.CLogger
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	once       sync.Once
}

func NewHeartbeatSender(gap time.Duration, client pb_meta.MetadataServiceClient, ni NodeInfo, replicator replication.Replicator) *HeartbeatSender {
	return &HeartbeatSender{
		gap:        gap,
		client:     client,
		nodeInfo:   ni,
		replicator: replicator,
		logger:     logging.NewCLogger().With(slog.String("component", "HeartbeatSender")),
	}
}

func (hs *HeartbeatSender) Start(ctx context.Context) {
	hs.once.Do(func() {
		hs.ctx, hs.cancel = context.WithCancel(ctx)
		hs.wg.Add(1)
		go hs.heartbeatTicker(hs.ctx)
	})
}

func (hs *HeartbeatSender) StopAndWait(ctx context.Context) {
	// cancel internal context
	hs.cancel()
	done := make(chan struct{})
	go func() {
		hs.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// wait done
	case <-ctx.Done():
		// wait ctx done -> return
		return
	}
}

func (hs *HeartbeatSender) heartbeatTicker(ctx context.Context) {
	ticker := time.NewTicker(hs.gap * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := hs.sendBeat(ctx)
			hs.logger.Warn("heartbeatTicker: failed to send beat", slog.String("err", err.Error()))
			// process the result
			hs.processHeartbeatResponse(ctx, res)
		}
	}
}

func (hs *HeartbeatSender) sendBeat(ctx context.Context) (*pb_meta.HeartbeatResponse, error) {
	reqCtx, cancel := context.WithTimeout(ctx, REQUEST_TIMEOUT*time.Second)
	defer cancel()
	req := &pb_meta.HeartbeatRequest{
		NodeId:     hs.nodeInfo.GetNodeID(),
		FreeSpace:  int64(hs.nodeInfo.GetFreeSpace()),
		ChunkCount: int32(hs.nodeInfo.GetChunkCount()),
	}
	res, err := hs.client.Heartbeat(reqCtx, req)
	hs.logger.Debug("heartbeat sent",
		"nodeId", req.NodeId,
		"freeSpace", req.FreeSpace,
		"chunkCount", req.ChunkCount,
	)

	if err != nil {
		return nil, err
	}
	return res, nil
}

func (hs *HeartbeatSender) processHeartbeatResponse(ctx context.Context, res *pb_meta.HeartbeatResponse) {
	outConc := make(chan struct{}, 10)

	droppedCount := 0
	for _, val := range res.RepairJobs {
		if ctx.Err() != nil {
			return
		}
		enqueued := hs.replicator.EnqueueRepair(replication.RepairJob{
			JobID:   val.JobId,
			ChunkID: val.ChunkId,
			Source:  val.SourceAddr,
			Target:  val.TargetAddr,
		})
		if !enqueued {
			droppedCount++
			// tell the metadata node about droped job
			go func() {

				select {
				case outConc <- struct{}{}:
					break
				case <-ctx.Done():
					return
				}

				defer func() {
					<-outConc
				}()
				timedCtx, cancel := context.WithTimeout(ctx, REQUEST_TIMEOUT*time.Second)
				defer cancel()
				_, err := hs.client.ReportRepairResult(timedCtx, &pb_meta.ReportRepairResultRequest{
					JobId:      val.JobId,
					JobSucceed: false,
					Error:      "job dropped: job queue is full",
					NodeId:     hs.nodeInfo.GetNodeID(),
				})
				if err != nil {
					hs.logger.Warn("dropped req metadata acknowledgement failed",
						slog.String("err", err.Error()),
						slog.String("job_id", val.JobId))
				}
			}()
		}
	}
	hs.logger.Debug("dropped_job", slog.Int("count", droppedCount))
	hs.logger.Debug("enqueued_job", slog.Int("count", len(res.RepairJobs)-droppedCount))
}
