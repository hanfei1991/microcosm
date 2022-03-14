package externalresource

import (
	"context"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

// ignoreAfterSuccClient will ignore the invocations after the first success,
// to reduce unnecessary requests.
// Currently it only implements PersistResource of MasterClient, calling other
// method will panic.
type ignoreAfterSuccClient struct {
	client.MasterClient
	success atomic.Bool
}

func (c *ignoreAfterSuccClient) UpdateResource(
	ctx context.Context,
	request *pb.UpdateResourceRequest,
) (*pb.UpdateResourceResponse, error) {
	if c.success.Load() {
		return &pb.UpdateResourceResponse{
			Error: &pb.ResourceError{ErrorCode: pb.ResourceErrorCode_ResourceOK},
		}, nil
	}
	resp, err := c.MasterClient.UpdateResource(ctx, request)
	if err != nil || resp.Error.ErrorCode != pb.ResourceErrorCode_ResourceOK {
		return resp, err
	}
	c.success.Store(true)
	return resp, nil
}

func LeaseTypeFromPB(leaseType pb.ResourceLeaseType) model.LeaseType {
	switch leaseType {
	case pb.ResourceLeaseType_LeaseBindToJob:
		return model.LeaseTypeJob
	case pb.ResourceLeaseType_LeaseBindToWorker:
		return model.LeaseTypeWorker
	case pb.ResourceLeaseType_LeaseTimeout:
		return model.LeaseTypeTimeout
	default:
	}
	log.L().Panic("unknown resource lease type", zap.Any("lease-type", leaseType))
	return ""
}
