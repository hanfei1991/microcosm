package dm

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.WorkerImpl = &dumpWorker{}

type dumpWorker struct {
	*lib.BaseWorker

	cfg        *config.SubTaskConfig
	core       *dumpling.Dumpling
	processCh  chan pb.ProcessResult
	lastResult *pb.ProcessResult

	lazyInitOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc
}

func (d *dumpWorker) InitImpl(ctx context.Context) error {
	d.core = dumpling.NewDumpling(d.cfg)
	d.processCh = make(chan pb.ProcessResult, 1)
	d.ctx, d.cancel = context.WithCancel(context.Background())
	err := d.core.Init(ctx)
	return errors.Trace(err)
}

func (d *dumpWorker) lazyInit() {
	go d.core.Process(d.ctx, d.processCh)
}

// TODO: what's the expected tick interval?
func (d *dumpWorker) Tick(ctx context.Context) error {
	d.lazyInitOnce.Do(d.lazyInit)

	return nil
}

func (d *dumpWorker) Status() lib.WorkerStatus {
	select {
	case result := <-d.processCh:
		d.lastResult = &result
	default:
	}

	status := d.core.Status(nil).(*pb.DumpStatus)

	switch {
	case d.lastResult == nil:
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal, Ext: status}
	case len(d.lastResult.Errors) > 0:
		return lib.WorkerStatus{
			Code:         lib.WorkerStatusError,
			ErrorMessage: d.lastResult.Errors[0].String(),
			Ext:          status,
		}
	default:
		return lib.WorkerStatus{Code: lib.WorkerStatusFinished, Ext: status}
	}
}

func (d *dumpWorker) Workload() model.RescUnit {
	log.L().Info("dumpWorker.Workload")
	return 0
}

func (d *dumpWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("dumpWorker.OnMasterFailover")
	return nil
}

func (d *dumpWorker) CloseImpl(ctx context.Context) error {
	d.core.Close()
	return nil
}
