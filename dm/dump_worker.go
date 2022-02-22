package dm

import (
	"context"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.WorkerImpl = &dumpWorker{}

type dumpWorker struct {
	*lib.DefaultBaseWorker

	cfg        *config.SubTaskConfig
	unitHolder *unitHolder
}

func newDumpWorker(cfg lib.WorkerConfig) lib.Worker {
	subtaskCfg := cfg.(*config.SubTaskConfig)
	return &dumpWorker{
		cfg: subtaskCfg,
	}
}

func (d *dumpWorker) Init(ctx context.Context) error {
	if err := d.DefaultBaseWorker.Init(ctx); err != nil {
		return errors.Trace(err)
	}

	d.unitHolder = newUnitHolder(dumpling.NewDumpling(d.cfg))
	return errors.Trace(d.unitHolder.init(ctx))
}

func (d *dumpWorker) Poll(ctx context.Context) error {
	if err := d.DefaultBaseWorker.Poll(ctx); err != nil {
		return errors.Trace(err)
	}

	d.unitHolder.lazyProcess()

	return nil
}

func (d *dumpWorker) Status() lib.WorkerStatus {
	hasResult, result := d.unitHolder.getResult()
	if !hasResult {
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal}
	}
	if len(result.Errors) > 0 {
		return lib.WorkerStatus{
			Code:         lib.WorkerStatusError,
			ErrorMessage: unit.JoinProcessErrors(result.Errors),
		}
	}
	return lib.WorkerStatus{Code: lib.WorkerStatusFinished}
}

func (d *dumpWorker) GetWorkerStatusExtTypeInfo() interface{} {
	return &struct{}{}
}

func (d *dumpWorker) Workload() model.RescUnit {
	log.L().Info("dumpWorker.Workload")
	return 0
}

func (d *dumpWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("dumpWorker.OnMasterFailover")
	return nil
}

func (d *dumpWorker) Close(ctx context.Context) error {
	if err := d.DefaultBaseWorker.Close(ctx); err != nil {
		log.L().Warn("failed to close BaseWorker", zap.Error(err))
	}

	d.unitHolder.close()
	return nil
}
