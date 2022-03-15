package externalresource

import (
	"context"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

type BaseProxy struct {
	ResourceID model.ResourceID
	ExecutorID model.ExecutorID
	WorkerID   model.WorkerID
	JobID      model.JobID

	MasterCli    client.MasterClient
	ResourceType ResourceType
}

func (p *BaseProxy) ID() model.ResourceID {
	return p.ResourceID
}

func (p *BaseProxy) Initialize(ctx context.Context) error {
	// Sends an RPC call to the server master to register the resource that
	// this proxy corresponds to.
	resp, err := p.MasterCli.CreateResource(ctx, &pb.CreateResourceRequest{
		ResourceId: p.ResourceID,
		ExecutorId: string(p.ExecutorID),
		JobId:      p.JobID,
		WorkerId:   p.WorkerID,
	})
	if err != nil {
		return errors.Trace(err)
	}
	switch resp.Error.ErrorCode {
	case pb.ResourceErrorCode_ResourceOK:
		break
	case pb.ResourceErrorCode_ResourceDuplicate:
		return derror.ErrDuplicateResources.GenWithStackByArgs(p.ResourceID)
	default:
		return derror.ErrCreateResourceFailed.GenWithStackByArgs(resp.GetError().String())
	}
	if resp.Error.BaseError != nil {
		return derror.ErrCreateResourceFailed.GenWithStackByArgs(resp.GetError().String())
	}
	return nil
}
