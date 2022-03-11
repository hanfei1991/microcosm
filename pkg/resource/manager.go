package resource

import (
	"context"

	 "github.com/hanfei1991/microcosm/pkg/resource/model"
)

type Manager struct {
	// resourceID -> executorID
	resourceMap map[string]string
}

func (m *Manager) UpsertResource(
	ctx context.Context,
	executorID model.ExecutorID,
	workerID model.WorkerID,
	resource ID,
	keepAlive model.KeepAliveStrategy,
) error {

}

func (m *Manager) ConsumeResource(
	ctx context.Context,
	executorID model.ExecutorID,
	workerID model.WorkerID,
	resource ID,
) error {

}
