package externalresource

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/externalresource/internal"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storage"
)

type ResourceType interface {
	IsBoundToExecutor() bool
	CleanUp(ctx context.Context, id model.ResourceID) error
	CreateStorage(ctx context.Context, baseProxy *internal.BaseProxy, suffix string) (storage.Proxy, error)
}
