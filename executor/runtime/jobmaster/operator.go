package jobmaster

import (
	"context"

	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/jobmaster/system"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/test"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type jobMasterAgent struct {
	metaKV metadata.MetaKV
	master system.JobMaster
}

func (j *jobMasterAgent) Prepare(ctx *runtime.TaskContext) error {
	if test.GlobalTestFlag {
		j.metaKV = ctx.TestCtx.GetMetaKV()
	}
	return j.master.Start(context.Background(), j.metaKV)
}

func (j *jobMasterAgent) Next(_ *runtime.TaskContext, _ *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	// TODO: Do something such as monitor the status of jobmaster.
	return nil, false, nil
}

func (j *jobMasterAgent) NextWantedInputIdx() int { return runtime.DontNeedData }

func (j *jobMasterAgent) Close() error {
	// "Stop" should not be blocked
	go func() {
		err := j.master.Stop(context.Background())
		log.L().Info("finish stop job", zap.Int64("id", int64(j.master.ID())), zap.Error(err))
	}()
	return nil
}