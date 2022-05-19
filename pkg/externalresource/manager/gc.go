package manager

import (
	"context"
	gerrors "errors"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/notifier"
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

		jobReceiver, executorReceiver, err := c.initializeGC(ctx)
		if err != nil {
			log.L().Warn("GC error", zap.Error(err))
			rl.Take()
			continue
		}

		err = c.runGCEventLoop(ctx, jobReceiver.C, executorReceiver.C)
		jobReceiver.Close()
		executorReceiver.Close()

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
	executorWatchCh <-chan model.ExecutorID,
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
		case offlinedExecutorID := <-executorWatchCh:
			err := c.gcByOfflineExecutorID(ctx, offlinedExecutorID)
			if err != nil {
				return err
			}
		}
	}
}

func (c *DefaultGCCoordinator) initializeGC(
	ctx context.Context,
) (*notifier.Receiver[JobStatusChangeEvent], *notifier.Receiver[model.ExecutorID], error) {
	jobSnapshot, jobWatchCh, err := c.jobInfos.WatchJobStatuses(ctx)
	if err != nil {
		return nil, nil, err
	}

	executorSnapshot, executorReceiver, err := c.executorInfos.WatchExecutors(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err := c.gcByStatusSnapshots(ctx, jobSnapshot, executorSnapshot); err != nil {
		return nil, nil, err
	}

	return jobWatchCh, executorReceiver, nil
}

func (c *DefaultGCCoordinator) gcByStatusSnapshots(
	ctx context.Context,
	jobSnapshot JobStatusesSnapshot,
	executorSnapshot []model.ExecutorID,
) error {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		log.L().Info("gcByStatusSnapshots finished",
			zap.Duration("duration", duration))
	}()

	resources, err := c.metaClient.QueryResources(ctx)
	if err != nil {
		return err
	}

	executorSet := make(map[model.ExecutorID]struct{}, len(executorSnapshot))
	for _, id := range executorSnapshot {
		executorSet[id] = struct{}{}
	}

	var (
		toGC     []resourcemeta.ResourceID
		toRemove []resourcemeta.ResourceID
	)
	for _, resMeta := range resources {
		if _, exists := jobSnapshot[resMeta.Job]; !exists {
			// The resource belongs to a deleted job.
			toGC = append(toGC, resMeta.ID)
			continue
		}

		if _, exists := executorSet[resMeta.Executor]; !exists {
			// The resource belongs to an offlined executor.
			toRemove = append(toGC, resMeta.ID)
			continue
		}
	}

	log.L().Info("Adding resources to GC queue",
		zap.Any("resource-ids", toGC))
	if err := c.metaClient.SetGCPending(ctx, toGC); err != nil {
		return err
	}

	log.L().Info("Removing stale resources for offlined executors",
		zap.Any("resource-ids", toRemove))
	if _, err := c.metaClient.DeleteResources(ctx, toRemove); err != nil {
		return err
	}

	return nil
}

func (c *DefaultGCCoordinator) gcByOfflineJobID(ctx context.Context, jobID string) error {
	resources, err := c.metaClient.QueryResourcesByJobID(ctx, jobID)
	if err != nil {
		return err
	}

	toGC := make([]resourcemeta.ResourceID, 0, len(resources))
	for _, resMeta := range resources {
		toGC = append(toGC, resMeta.ID)
	}

	log.L().Info("Added resources to GC queue",
		zap.Any("resource-ids", toGC))

	return c.metaClient.SetGCPending(ctx, toGC)
}

func (c *DefaultGCCoordinator) gcByOfflineExecutorID(ctx context.Context, executorID model.ExecutorID) error {
	log.L().Info("Cleaning up resources meta for offlined executor",
		zap.String("executor-id", string(executorID)))
	return c.metaClient.DeleteResourcesByExecutorID(ctx, string(executorID))
}
