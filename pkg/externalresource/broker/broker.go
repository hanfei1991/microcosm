package broker

import (
	"context"

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pb"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
)

type DefaultBroker struct {
	config     *storagecfg.Config
	executorID resModel.ExecutorID
	client     *rpcutil.FailoverRPCClients[pb.ResourceManagerClient]

	fileManager FileManager
}

func NewBroker(
	config *storagecfg.Config,
	executorID resModel.ExecutorID,
	client *rpcutil.FailoverRPCClients[pb.ResourceManagerClient],
) *DefaultBroker {
	fm := NewLocalFileManager(*config.Local)
	return &DefaultBroker{
		config:      config,
		executorID:  executorID,
		client:      client,
		fileManager: fm,
	}
}

func (b *DefaultBroker) OpenStorage(
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
		return b.newHandleForLocalFile(ctx, jobID, workerID, resourcePath)
	case resModel.ResourceTypeS3:
		log.L().Panic("resource type s3 is not supported for now")
	default:
	}

	log.L().Panic("unsupported resource type", zap.String("resource-path", resourcePath))
	panic("unreachable")
}

func (b *DefaultBroker) OnWorkerClosed(ctx context.Context, workerID resModel.WorkerID, jobID resModel.JobID) {
	err := b.fileManager.RemoveTemporaryFiles(workerID)
	if err != nil {
		// TODO when we have a cloud-based error collection service, we need
		// to report this.
		// However, since an error here is unlikely to indicate a correctness
		// problem, we do not take further actions.
		log.L().Warn("Failed to remove temporary files for worker",
			zap.String("worker-id", workerID),
			zap.String("job-id", jobID),
			zap.Error(err))
	}
}

func (b *DefaultBroker) newHandleForLocalFile(
	ctx context.Context,
	jobID resModel.JobID,
	workerID resModel.WorkerID,
	resourceID resModel.ResourceID,
) (hdl Handle, retErr error) {
	// Note the semantics of ParseResourcePath:
	// If resourceID is `/local/my-resource`, then tp == resModel.ResourceTypeLocalFile
	// and resName == "my-resource".
	tp, resName, err := resModel.ParseResourcePath(resourceID)
	if err != nil {
		return nil, err
	}
	if tp != resModel.ResourceTypeLocalFile {
		log.L().Panic("unexpected resource type", zap.String("type", string(tp)))
	}

	record, exists, err := b.checkForExistingResource(ctx, resourceID)
	if err != nil {
		return nil, err
	}

	var (
		res             *resModel.LocalFileResourceDescriptor
		creatorWorkerID libModel.WorkerID
	)

	if !exists {
		creatorWorkerID = workerID
		res, err = b.fileManager.CreateResource(workerID, resName)
		if err != nil {
			return nil, err
		}
		defer func() {
			if retErr != nil {
				//nolint:errcheck
				_ = b.fileManager.RemoveResource(workerID, resName)
			}
		}()
	} else {
		creatorWorkerID = record.Worker
		res, err = b.fileManager.GetResource(record.Worker, resName)
		if err != nil {
			return nil, err
		}
	}

	filePath := res.AbsolutePath()
	log.L().Info("Using local storage with path", zap.String("path", filePath))

	ls, err := newBrStorageForLocalFile(filePath)
	if err != nil {
		return nil, err
	}

	return &BrExternalStorageHandle{
		inner:  ls,
		client: b.client,

		id:         resourceID,
		jobID:      jobID,
		workerID:   creatorWorkerID,
		executorID: b.executorID,
	}, nil
}

func (b *DefaultBroker) checkForExistingResource(
	ctx context.Context,
	resourceID resModel.ResourceID,
) (*resModel.ResourceMeta, bool, error) {
	resp, err := rpcutil.DoFailoverRPC(
		ctx,
		b.client,
		&pb.QueryResourceRequest{ResourceId: resourceID},
		pb.ResourceManagerClient.QueryResource,
	)
	if err == nil {
		return &resModel.ResourceMeta{
			ID:       resourceID,
			Job:      resp.GetJobId(),
			Worker:   resp.GetCreatorWorkerId(),
			Executor: resModel.ExecutorID(resp.GetCreatorExecutor()),
			Deleted:  false,
		}, true, nil
	}

	// TODO perhaps we need a grpcutil package to put all this stuff?
	st, ok := status.FromError(err)
	if !ok {
		// If the error is not derived from a grpc status, we should throw it.
		return nil, false, errors.Trace(err)
	}
	if len(st.Details()) != 1 {
		// The resource manager only generates status with ONE detail.
		return nil, false, errors.Trace(err)
	}
	resourceErr, ok := st.Details()[0].(*pb.ResourceError)
	if !ok {
		return nil, false, errors.Trace(err)
	}

	log.L().Info("Got ResourceError",
		zap.String("resource-id", resourceID),
		zap.Any("resource-err", resourceErr))
	switch resourceErr.ErrorCode {
	case pb.ResourceErrorCode_ResourceNotFound:
		// Indicates that there is no existing resource with the same name.
		return nil, false, nil
	default:
		log.L().Warn("Unexpected ResourceError",
			zap.String("code", resourceErr.ErrorCode.String()),
			zap.String("stack-trace", resourceErr.StackTrace))
		return nil, false, errors.Trace(err)
	}
}
