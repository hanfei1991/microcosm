package resourcemeta

import (
	"context"

	"go.uber.org/ratelimit"

	"github.com/hanfei1991/microcosm/model"
	dorm "github.com/hanfei1991/microcosm/pkg/meta/orm"
	libModel "github.com/hanfei1991/microcosm/pkg/meta/orm/model"
)

const (
	metadataQPSLimit = 1024
)

type MetadataAccessor struct {
	// rl limits the frequency the metastore is written to.
	// It helps to prevent cascading failures after a fail-over
	// where a large number of resources are created.
	rl         ratelimit.Limiter
	metaclient *dorm.MetaOpsClient
}

func NewMetadataAccessor(client *dorm.MetaOpsClient) *MetadataAccessor {
	return &MetadataAccessor{
		rl:         ratelimit.New(metadataQPSLimit),
		metaclient: client,
	}
}

func (m *MetadataAccessor) GetResource(ctx context.Context, resourceID ResourceID) (*libModel.ResourceMeta, bool, error) {
	rec, err := m.metaclient.GetResourceByID(ctx, resourceID)
	// TODO
	//if derror.ErrDatasetEntryNotFound.Equal(err) {
	//	return nil, false, nil
	//}
	//if err != nil {
	//	return nil, false, err
	//}
	return rec, true, nil
}

func (m *MetadataAccessor) CreateResource(ctx context.Context, resource *libModel.ResourceMeta) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resource.ID)
	if err == nil {
		// A duplicate exists
		return false, nil
	}
	// TODO
	//if !derror.ErrDatasetEntryNotFound.Equal(err) {
	// An unexpected error
	//return false, err
	//}

	m.rl.Take()
	if err := m.metaclient.AddResource(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) UpdateResource(ctx context.Context, resource *libModel.ResourceMeta) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resource.ID)
	//if derror.ErrDatasetEntryNotFound.Equal(err) {
	//	return false, nil
	//}
	if err != nil {
		return false, err
	}

	m.rl.Take()
	if err := m.metaclient.UpdateResource(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

func (m *MetadataAccessor) DeleteResource(ctx context.Context, resourceID ResourceID) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resourceID)
	//if derror.ErrDatasetEntryNotFound.Equal(err) {
	//	return false, nil
	//}
	if err != nil {
		return false, err
	}

	if err := m.metaclient.DeleteResource(ctx, resourceID); err != nil {
		return false, err
	}
	return true, nil
}

func (m *MetadataAccessor) GetResourcesForExecutor(
	ctx context.Context,
	executorID model.ExecutorID,
) ([]*libModel.ResourceMeta, error) {
	return m.metaclient.QueryResourcesByExecutorID(ctx, executorID)
}
