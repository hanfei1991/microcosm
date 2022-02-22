package fake

import (
	"context"
	"errors"
	"sync/atomic"

	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
)

var _ lib.Worker = (*dummyWorker)(nil)

type (
	Worker      = dummyWorker
	dummyWorker struct {
		lib.BaseWorker

		init   bool
		closed int32
		tick   int64
	}
)

type dummyStatus struct{}

func (d *dummyWorker) Init(ctx context.Context) error {
	if err := d.BaseWorker.Init(ctx); err != nil {
		return err
	}

	if !d.init {
		d.init = true
		return nil
	}
	return errors.New("repeated init")
}

func (d *dummyWorker) Poll(ctx context.Context) error {
	if err := d.BaseWorker.Poll(ctx); err != nil {
		return err
	}

	if !d.init {
		return errors.New("not yet init")
	}

	if d.tick%200 == 0 {
		log.L().Info("FakeWorker: Poll", zap.String("worker-id", d.ID()), zap.Int64("tick", d.tick))
	}
	if atomic.LoadInt32(&d.closed) == 1 {
		return nil
	}
	d.tick++
	return nil
}

func (d *dummyWorker) Status() lib.WorkerStatus {
	if d.init {
		return lib.WorkerStatus{Code: lib.WorkerStatusNormal, Ext: d.tick}
	}
	return lib.WorkerStatus{Code: lib.WorkerStatusCreated}
}

func (d *dummyWorker) Workload() model.RescUnit {
	return model.RescUnit(10)
}

func (d *dummyWorker) OnMasterFailover(_ lib.MasterFailoverReason) error {
	return nil
}

func (d *dummyWorker) GetWorkerStatusExtTypeInfo() interface{} {
	return &dummyStatus{}
}

func (d *dummyWorker) Close(ctx context.Context) error {
	if err := d.BaseWorker.Close(ctx); err != nil {
		log.L().Warn("Failed to close BaseWorker", zap.Error(err))
	}

	atomic.StoreInt32(&d.closed, 1)
	return nil
}

func NewDummyWorker(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, _ lib.WorkerConfig) lib.Worker {
	ret := &dummyWorker{}
	dependencies := ctx.Dependencies
	ret.BaseWorker = lib.NewBaseWorker(
		ret,
		dependencies.MessageHandlerManager,
		dependencies.MessageRouter,
		dependencies.MetaKVClient,
		id,
		masterID)

	return ret
}
