package externalresource

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/pkg/ctxmu"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/internal"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
)

type Manager struct {
	mu            ctxmu.CtxMutex
	metaAccessor  *internal.MetadataAccessor
	gcCoordinator *internal.GCCoordinator

	wg       sync.WaitGroup
	cancelGC context.CancelFunc
}

// NewManager returns a new Manager.
func NewManager(client metaclient.KV) *Manager {
	gcCtx, cancel := context.WithCancel(context.Background())
	ret := &Manager{
		mu:            ctxmu.New(),
		metaAccessor:  internal.NewMetadataAccessor(client),
		gcCoordinator: &internal.GCCoordinator{},
		cancelGC:      cancel,
	}
	ret.launchGCTask(gcCtx)
	return ret
}

// CreateResource creates a new resource.
// NOTE the default LeaseType is LeaseTypeWorker.
func (m *Manager) CreateResource(
	ctx context.Context,
	resourceID model.ResourceID,
	jobID model.JobID,
	executorID model.ExecutorID,
	workerID model.WorkerID,
) error {
	if !m.mu.Lock(ctx) {
		return errors.Trace(ctx.Err())
	}
	defer m.mu.Unlock()

	resourceMeta := &model.ResourceMeta{
		ID:        resourceID,
		LeaseType: model.LeaseTypeWorker,
		Job:       jobID,
		Worker:    workerID,
		Executor:  executorID,
	}

	ok, err := m.metaAccessor.CreateResource(ctx, resourceMeta)
	if !ok {
		return derror.ErrDuplicateResources.GenWithStackByArgs(resourceID)
	}
	return err
}

// UpdateResource updates a resource.
// NOTE that only the workerID that the resource binds to,
// or the lease type can be changed.
func (m *Manager) UpdateResource(
	ctx context.Context,
	resourceID model.ResourceID,
	workerID model.WorkerID,
	leaseType model.LeaseType,
) error {
	if !m.mu.Lock(ctx) {
		return errors.Trace(ctx.Err())
	}
	defer m.mu.Unlock()

	resource, ok, err := m.metaAccessor.GetResource(ctx, resourceID)
	if err != nil {
		return err
	}
	if !ok {
		return derror.ErrResourceNotFound.GenWithStackByArgs(resourceID)
	}

	resource.Worker = workerID
	resource.LeaseType = leaseType

	ok, err = m.metaAccessor.UpdateResource(ctx, resource)
	if err != nil {
		return err
	}
	if !ok {
		return derror.ErrResourceMetaCorrupted.GenWithStackByArgs(resourceID)
	}
	return nil
}

func (m *Manager) OnWorkerClosed(
	ctx context.Context,
	workerID model.WorkerID,
) error {
	if !m.mu.Lock(ctx) {
		return errors.Trace(ctx.Err())
	}
	defer m.mu.Unlock()

	// TODO implement me

	log.L().Info("ExternalResourceManager: OnWorkerClosed", zap.String("worker-id", workerID))
	return nil
}

// RemoveResource removes a resource.
func (m *Manager) RemoveResource(
	ctx context.Context,
	resourceID model.ResourceID,
) error {
	// TODO
	return nil
}

func (m *Manager) OnExecutorOffline(executorID model.ExecutorID) {
	// TODO
}

// GetExecutorConstraint returns an executor ID that a worker trying to
// consume the given resource must run on.
func (m *Manager) GetExecutorConstraint(
	ctx context.Context,
	resourceID model.ResourceID,
) (model.ExecutorID, error) {
	if !m.mu.Lock(ctx) {
		return "", errors.Trace(ctx.Err())
	}
	defer m.mu.Unlock()

	resource, ok, err := m.metaAccessor.GetResource(ctx, resourceID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", derror.ErrResourceNotFound.GenWithStackByArgs(resourceID)
	}

	return resource.Executor, nil
}

// Stop stops the background GC task.
func (m *Manager) Stop() {
	m.cancelGC()
	m.wg.Wait()
}

func (m *Manager) launchGCTask(ctx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer log.L().Info("Resource GC task is canceled")

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.doGC(ctx); err != nil {
					return
				}
			}
		}
	}()
}

func (m *Manager) doGC(ctx context.Context) error {
	if !m.mu.Lock(ctx) {
		return errors.Trace(ctx.Err())
	}
	defer m.mu.Unlock()

	m.gcCoordinator.Tick(ctx)
	return nil
}
