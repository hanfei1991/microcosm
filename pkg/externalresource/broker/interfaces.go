package broker

import (
	"context"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
)

// A Broker is created and maintained by the executor
// and provides file resources to the tasks.
type Broker interface {
	OpenStorage(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
		resourcePath resModel.ResourceID,
	) (Handle, error)

	OnWorkerClosed(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
	)
}

// FileManager abstracts the operations on resources that
// a Broker needs to perform.
type FileManager interface {
	CreateResource(
		creator libModel.WorkerID,
		resourceID resModel.ResourceID,
	) (string, error)

	GetPathForResource(
		creator libModel.WorkerID,
		resourceID resModel.ResourceID,
	) (string, error)

	RemoveTemporaryFiles(creator libModel.WorkerID) error
	RemoveResource(resourceID resModel.ResourceID) error
}
