package internal

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"github.com/hanfei1991/microcosm/pkg/resource/model"
)

type MetadataAccessor struct {
	client metaclient.KV
}

func (m *MetadataAccessor) CreateResource(ctx context.Context, resource *model.ResourceMeta) (bool, error) {
	key := adapter.ResourceKeyAdapter.Encode(resource.ID)
	resp, err := m.client.Get(ctx, adapter.ResourceKeyAdapter.Encode(resource.ID))
	if err != nil {
		return false, err
	}

	if len(resp.Kvs) == 1 {
		// Resource already exists
		return false, nil
	}
	if len(resp.Kvs) > 1 {
		log.L().Panic("unreachable", zap.Any("resp", resp))
	}

	str, err1 := json.Marshal(resource)
	if err1 != nil {
		return false, errors.Trace(err)
	}

	m.client.Txn(ctx).Do(metaclient.OpPut(key, string(str)))
}

func (m *MetadataAccessor) UpdateResource(ctx context.Context, resource *model.ResourceMeta) (bool, error) {

}

func (m *MetadataAccessor) DeleteResource(ctx context.Context, resource model.ResourceID) (bool, error) {

}

func (m *MetadataAccessor) DeleteResourcesForExecutor(
	ctx context.Context,
	executorID model.ExecutorID,
) (count int, err error) {

}