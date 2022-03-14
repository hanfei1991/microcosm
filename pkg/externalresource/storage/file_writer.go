package storage

import (
	"context"
	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type fileWriter struct {
	storage.ExternalFileWriter

	resourceID model.ResourceID
	executorID model.ExecutorID
	workerID   model.WorkerID
	masterCli  client.MasterClient
}

func (f *fileWriter) Close(ctx context.Context) error {
	err := f.ExternalFileWriter.Close(ctx)
	if err != nil {
		return err
	}
	// failing here will generate trash files, need clean it
	resp, err := f.masterCli.UpdateResource(ctx, &pb.UpdateResourceRequest{
		WorkerId:   f.workerID,
		ResourceId: f.resourceID,
		// We are using the timeout lease as the default lease for persistence.
		// The alternative may be a job lease.
		LeaseType: pb.ResourceLeaseType_LeaseTimeout,
	})
	if err != nil {
		return errors.New(resp.Error.String())
	}
	return nil
}
