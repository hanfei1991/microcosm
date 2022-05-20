package dmtask

import (
	"context"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/pingcap/tiflow/dm/dm/config"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/syncer"
)

// SyncTask represents a sync task
type SyncTask struct {
	BaseDMTask
}

// newSyncTask create a sync task
func newSyncTask(baseDMTask BaseDMTask) lib.WorkerImpl {
	syncTask := &SyncTask{
		BaseDMTask: baseDMTask,
	}
	syncTask.BaseDMTask.DMTask = syncTask
	return syncTask
}

// onInit implements DMTask.onInit
func (t *SyncTask) onInit(ctx context.Context) error {
	if t.cfg.Mode == dmconfig.ModeAll {
		return t.setupStorge(ctx)
	}
	return nil
}

// onFinished implements DMTask.onFinished
// Should not happened.
func (t *SyncTask) onFinished(ctx context.Context) error {
	return nil
}

// createUnitHolder implements DMTask.createUnitHolder
func (t *SyncTask) createUnitHolder(cfg *config.SubTaskConfig) *unitHolder {
	return newUnitHolder(lib.WorkerDMSync, cfg.SourceID, syncer.NewSyncer(cfg, nil, nil))
}
