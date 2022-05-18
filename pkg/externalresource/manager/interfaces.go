package manager

import (
	"context"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/notifier"
)

// ExecutorInfoProvider describes an object that maintains a list
// of all executors
type ExecutorInfoProvider interface {
	HasExecutor(executorID string) bool
	ListExecutors() []string
	WatchExecutors(
		ctx context.Context,
	) ([]model.ExecutorID, *notifier.Receiver[model.ExecutorID], error)
}

// JobStatus describes the a Job's status.
type JobStatus = libModel.MasterStatusCode

// JobStatusesSnapshot describes the statuses of all jobs
// at some time point.
type JobStatusesSnapshot = map[libModel.MasterID]JobStatus

// JobStatusChangeType describes the type of job status changes.
type JobStatusChangeType int32

const (
	// JobRemovedEvent means that a job has been removed.
	JobRemovedEvent = JobStatusChangeType(iota + 1)
)

// JobStatusChangeEvent is an event denoting a job status
// has changed.
type JobStatusChangeEvent struct {
	EventType JobStatusChangeType
	JobID     libModel.MasterID
}

// JobStatusProvider describes an object that can be queried
// on the status of jobs.
type JobStatusProvider interface {
	// GetJobStatuses returns the status of all jobs that are
	// not deleted.
	GetJobStatuses(ctx context.Context) (JobStatusesSnapshot, error)

	// WatchJobStatuses listens on all job status changes followed by
	// a snapshot.
	WatchJobStatuses(ctx context.Context) (JobStatusesSnapshot, <-chan JobStatusChangeEvent, error)
}

// GCCoordinator describes an object responsible for triggering
// file resource garbage collection.
type GCCoordinator interface {
	Run(ctx context.Context) error
	OnKeepAlive(resourceID resourcemeta.ResourceID, workerID libModel.WorkerID)
}
