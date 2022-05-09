package broker

import (
	"context"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pb"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
)

type DefaultBroker struct {
	config     *storagecfg.Config
	executorID resModel.ExecutorID

	factory *Factory
}

func NewBroker(
	config *storagecfg.Config,
	executorID resModel.ExecutorID,
	client *rpcutil.FailoverRPCClients[pb.ResourceManagerClient],
) *DefaultBroker {
	return &DefaultBroker{
		config:     config,
		executorID: executorID,
		factory: &Factory{
			config:     config,
			client:     client,
			executorID: executorID,
		},
	}
}

func (i *DefaultBroker) OpenStorage(
	ctx context.Context,
	workerID resModel.WorkerID,
	jobID resModel.JobID,
	resourcePath resModel.ResourceID,
) (Handle, error) {
	tp, _, err := resModel.ParseResourcePath(resourcePath)
	if err != nil {
		return nil, err
	}

	switch tp {
	case resModel.ResourceTypeLocalFile:
		return i.factory.NewHandleForLocalFile(ctx, jobID, workerID, resourcePath)
	case resModel.ResourceTypeS3:
		log.L().Panic("resource type s3 is not supported for now")
	default:
	}

	log.L().Panic("unsupported resource type", zap.String("resource-path", resourcePath))
	panic("unreachable")
}

func (i *DefaultBroker) OnWorkerClosed(ctx context.Context, workerID resModel.WorkerID, jobID resModel.JobID) {
	panic("implement me")
}
