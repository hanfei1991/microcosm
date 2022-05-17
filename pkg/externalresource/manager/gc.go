package manager

import (
	"context"
	gerrors "errors"

	"github.com/pingcap/errors"
	"go.uber.org/ratelimit"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
)

// DefaultGCCoordinator implements interface GCCoordinator.
// It is responsible for triggering file resource garbage collection.
type DefaultGCCoordinator struct {
	executorInfos ExecutorInfoProvider
	jobInfos      JobStatusProvider
	metaClient    pkgOrm.ResourceClient
}

// NewGCCoordinator creates a new DefaultGCCoordinator.
func NewGCCoordinator(
	executorInfos ExecutorInfoProvider,
	jobInfos JobStatusProvider,
	metaClient pkgOrm.ResourceClient,
) *DefaultGCCoordinator {
	return &DefaultGCCoordinator{
		executorInfos: executorInfos,
		jobInfos:      jobInfos,
		metaClient:    metaClient,
	}
}

// Run runs the DefaultGCCoordinator.
func (c *DefaultGCCoordinator) Run(ctx context.Context) error {
	// We run a retry loop at the max frequency of once per second.
	rl := ratelimit.New(1 /* once per second */)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		jobWatchCh, err := c.initializeGC(ctx)
		if err != nil {
			rl.Take()
			continue
		}

		err = c.runGCEventLoop(ctx, jobWatchCh)
		if gerrors.Is(err, context.Canceled) || gerrors.Is(err, context.DeadlineExceeded) {
			return errors.Trace(err)
		}
		rl.Take()
	}
}

// OnKeepAlive is not implemented for now.
func (c *DefaultGCCoordinator) OnKeepAlive(resourceID resourcemeta.ResourceID, workerID libModel.WorkerID) {
	// TODO implement me
	panic("implement me")
}

func (c *DefaultGCCoordinator) runGCEventLoop(
	ctx context.Context,
	jobWatchCh <-chan JobStatusChangeEvent,
) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case jobStatusChange := <-jobWatchCh:
			if jobStatusChange.EventType != JobRemovedEvent {
				continue
			}
			err := c.gcByOfflineJobID(ctx, jobStatusChange.JobID)
			if err != nil {
				return err
			}
			// TODO listen on executor offlines.
		}
	}
}

func (c *DefaultGCCoordinator) initializeGC(ctx context.Context) (<-chan JobStatusChangeEvent, error) {
	snapshot, jobWatchCh, err := c.jobInfos.WatchJobStatuses(ctx)
	if err != nil {
		return nil, err
	}

	if err := c.gcByAllJobStatusSnapshot(ctx, snapshot); err != nil {
		return nil, err
	}

	return jobWatchCh, nil
}

func (c *DefaultGCCoordinator) gcByAllJobStatusSnapshot(ctx context.Context, snapshot JobStatusesSnapshot) error {
	resources, err := c.metaClient.QueryResources(ctx)
	if err != nil {
		return err
	}

	var toGC []resourcemeta.ResourceID
	for _, resMeta := range resources {
		if _, exists := snapshot[resMeta.Job]; !exists {
			// The resource belongs to a deleted job.
			toGC = append(toGC, resMeta.ID)
		}
	}

}

func (c *DefaultGCCoordinator) gcByOfflineJobID(ctx context.Context, jobID string) error {
	// TODO implement me
	panic("implement me")
}
