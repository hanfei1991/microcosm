package dm

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.WorkerImpl = &loadWorker{}

type loadWorker struct {
	*lib.BaseWorker

	cfg        *config.SubTaskConfig
	workerName string
	core       *loader.LightningLoader
	processCh  chan pb.ProcessResult
	lastResult *pb.ProcessResult

	lazyInitOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc
}

func (l *loadWorker) InitImpl(ctx context.Context) error {
	l.core = loader.NewLightning(l.cfg, l.BaseWorker.MetaKVClient(), l.workerName)
	l.processCh = make(chan pb.ProcessResult, 1)
	l.ctx, l.cancel = context.WithCancel(context.Background())
	err := l.core.Init(ctx)
	return errors.Trace(err)
}

func (l *loadWorker) lazyInit() {
	go l.core.Process(l.ctx, l.processCh)
}

// TODO: what's the expected tick interval?
func (l *loadWorker) Tick(ctx context.Context) error {
	l.lazyInitOnce.Do(l.lazyInit)

	return nil
}

func (l *loadWorker) Status() lib.WorkerStatus {
	select {
	case result := <-l.processCh:
		l.lastResult = &result
	default:
	}

	status := l.core.Status(nil).(*pb.DumpStatus)

	switch {
	case l.lastResult == nil:
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal, Ext: status}
	case len(l.lastResult.Errors) > 0:
		return lib.WorkerStatus{
			Code:         lib.WorkerStatusError,
			ErrorMessage: l.lastResult.Errors[0].String(),
			Ext:          status,
		}
	default:
		return lib.WorkerStatus{Code: lib.WorkerStatusFinished, Ext: status}
	}
}

func (l *loadWorker) Workload() model.RescUnit {
	log.L().Info("dumpWorker.Workload")
	return 0
}

func (l *loadWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("dumpWorker.OnMasterFailover")
	return nil
}

func (l *loadWorker) CloseImpl(ctx context.Context) error {
	l.core.Close()
	return nil
}
