package server

import (
	"context"
	pb "github.com/satyam709/distributed-fs/gen/proto/metadata/v1"
)

func (h *MetadataServiceHandler) ReportRepairFailure(ctx context.Context, req *pb.ReportRepairFailureRequest) (*pb.ReportRepairFailureResponse, error) {
	if !h.isLeader() {
		return nil, h.leaderRedirect()
	}
	return nil, nil
}
