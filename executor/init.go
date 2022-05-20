package executor

import (
	cvstask "github.com/hanfei1991/microcosm/executor/cvsTask"
	dmtask "github.com/hanfei1991/microcosm/executor/dmTask"
	cvs "github.com/hanfei1991/microcosm/jobmaster/cvsJob"
	"github.com/hanfei1991/microcosm/jobmaster/dm"
	"github.com/hanfei1991/microcosm/lib/registry"
)

func init() {
	cvstask.RegisterWorker()
	cvs.RegisterWorker()
	dm.RegisterWorker()
	dmtask.RegisterWorker()
	registry.RegisterFake(registry.GlobalWorkerRegistry())
}
