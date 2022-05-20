package dmtask

import (
	"context"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/loader"
)

// LoadTask represents a load task
type LoadTask struct {
	BaseDMTask
}

// newLoadTask create a load task
func newLoadTask(baseDMTask BaseDMTask) lib.WorkerImpl {
	loadTask := &LoadTask{
		BaseDMTask: baseDMTask,
	}
	loadTask.BaseDMTask.DMTask = loadTask
	return loadTask
}

// onInit implements DMTask.onInit
func (t *LoadTask) onInit(ctx context.Context) error {
	return t.setupStorge(ctx)
}

// onFinished implements DMTask.onFinished
func (t *LoadTask) onFinished(ctx context.Context) error {
	return nil
}

// createUnitHolder implements DMTask.createUnitHolder
func (t *LoadTask) createUnitHolder(cfg *config.SubTaskConfig) *unitHolder {
	// `workerName` and `etcdClient` of `NewLightning` are not used in dataflow
	// scenario, we just use readable values here.
	workerName := "dataflow-worker"
	return newUnitHolder(lib.WorkerDMLoad, cfg.SourceID, loader.NewLightning(cfg, nil, workerName))
}
