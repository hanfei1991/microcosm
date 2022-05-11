package broker

import (
	"context"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"

	"github.com/hanfei1991/microcosm/pb"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
)

type Handle interface {
	ID() resModel.ResourceID
	BrExternalStorage() brStorage.ExternalStorage
	Persist(ctx context.Context) error
	Discard(ctx context.Context) error
}

// BrExternalStorageHandle contains a brStorage.ExternalStorage.
// It helps Dataflow Engine reuse the external storage facilities
// implemented in Br.
type BrExternalStorageHandle struct {
	id         resModel.ResourceID
	jobID      resModel.JobID
	workerID   resModel.WorkerID
	executorID resModel.ExecutorID

	inner  brStorage.ExternalStorage
	client *rpcutil.FailoverRPCClients[pb.ResourceManagerClient]
}

func (h *BrExternalStorageHandle) ID() resModel.ResourceID {
	return h.id
}

func (h *BrExternalStorageHandle) BrExternalStorage() brStorage.ExternalStorage {
	return h.inner
}

func (h *BrExternalStorageHandle) Persist(ctx context.Context) error {
	_, err := rpcutil.DoFailoverRPC(
		ctx,
		h.client,
		&pb.CreateResourceRequest{
			ResourceId:      h.id,
			CreatorExecutor: string(h.executorID),
			JobId:           h.jobID,
			CreatorWorkerId: h.workerID,
		},
		pb.ResourceManagerClient.CreateResource,
	)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *BrExternalStorageHandle) Discard(ctx context.Context) error {
	return nil
}
