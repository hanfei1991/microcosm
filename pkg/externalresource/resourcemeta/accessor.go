package resourcemeta

import (
	"context"

	"go.uber.org/ratelimit"

	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	dorm "github.com/hanfei1991/microcosm/pkg/orm"
)

const (
	metadataQPSLimit = 1024
)

type MetadataAccessor struct {
	// rl limits the frequency the metastore is written to.
	// It helps to prevent cascading failures after a fail-over
	// where a large number of resources are created.
	rl         ratelimit.Limiter
	metaclient dorm.Client
}

func NewMetadataAccessor(client dorm.Client) *MetadataAccessor {
	return &MetadataAccessor{
		rl:         ratelimit.New(metadataQPSLimit),
		metaclient: client,
	}
}

func (m *MetadataAccessor) GetResource(ctx context.Context, resourceID resModel.ResourceID) (*resModel.ResourceMeta, bool, error) {
	rec, err := m.metaclient.GetResourceByID(ctx, resourceID)
	if err == nil {
		return rec, true, nil
	}

	if dorm.IsNotFoundError(err) {
		return nil, false, nil
	}

	return nil, false, err
}

func (m *MetadataAccessor) CreateResource(ctx context.Context, resource *resModel.ResourceMeta) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resource.ID)
	if err == nil {
		// A duplicate exists
		return false, nil
	}
	if !dorm.IsNotFoundError(err) {
		// An unexpected error
		return false, err
	}

	m.rl.Take()
	if err := m.metaclient.UpsertResource(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) UpdateResource(ctx context.Context, resource *resModel.ResourceMeta) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resource.ID)
	if err != nil {
		if dorm.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	m.rl.Take()
	if err := m.metaclient.UpdateResource(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) DeleteResource(ctx context.Context, resourceID resModel.ResourceID) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resourceID)
	if err != nil {
		if dorm.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	if err := m.metaclient.DeleteResource(ctx, resourceID); err != nil {
		return false, err
	}
	return true, nil
}

func (m *MetadataAccessor) GetAllResources(ctx context.Context) ([]*resModel.ResourceMeta, error) {
	return m.metaclient.QueryResources(ctx)
}

func (m *MetadataAccessor) GetResourcesForExecutor(
	ctx context.Context,
	executorID resModel.ExecutorID,
) ([]*resModel.ResourceMeta, error) {
	return m.metaclient.QueryResourcesByExecutorID(ctx, string(executorID))
}
