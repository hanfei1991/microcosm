package externalresource

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

type FileWriterImpl struct {
	storage.ExternalFileWriter

	ResourceID model.ResourceID
	ExecutorID model.ExecutorID
	WorkerID   model.WorkerID
	MasterCli  client.MasterClient
}

func (f *FileWriterImpl) Close(ctx context.Context) error {
	err := f.ExternalFileWriter.Close(ctx)
	if err != nil {
		return err
	}

	if f.MasterCli == nil {
		return nil
	}

	// failing here will generate trash files, need clean it
	resp, err := f.MasterCli.UpdateResource(ctx, &pb.UpdateResourceRequest{
		WorkerId:   f.WorkerID,
		ResourceId: f.ResourceID,
		// We are using the timeout lease as the default lease for persistence.
		// The alternative may be a job lease.
		LeaseType: pb.ResourceLeaseType_LeaseTimeout,
	})
	if err != nil {
		return errors.New(resp.Error.String())
	}
	return nil
}
