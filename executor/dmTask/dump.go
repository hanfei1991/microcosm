package dmtask

import (
	"context"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dumpling"
)

// DumpTask represents a dump task
type DumpTask struct {
	BaseDMTask
}

// newDumpTask create a dump task
func newDumpTask(baseDMTask BaseDMTask) lib.WorkerImpl {
	dumpTask := &DumpTask{
		BaseDMTask: baseDMTask,
	}
	dumpTask.BaseDMTask.DMTask = dumpTask
	return dumpTask
}

// onInit implements DMTask.onInit
func (t *DumpTask) onInit(ctx context.Context) error {
	return t.setupStorge(ctx)
}

// onFinished implements DMTask.onFinished
func (t *DumpTask) onFinished(ctx context.Context) error {
	return t.persistStorge(ctx)
}

// createUnitHolder implements DMTask.createUnitHolder
func (t *DumpTask) createUnitHolder(cfg *config.SubTaskConfig) *unitHolder {
	return newUnitHolder(lib.WorkerDMDump, cfg.SourceID, dumpling.NewDumpling(cfg))
}
