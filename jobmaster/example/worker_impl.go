package example

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
)

var _ lib.Worker = &exampleWorker{}

var tickKey = "tick_count"

type exampleWorker struct {
	*lib.DefaultBaseWorker

	work struct {
		mu        sync.Mutex
		tickCount int
		finished  bool
	}
	wg sync.WaitGroup
}

func (w *exampleWorker) run() {
	defer w.wg.Done()

	time.Sleep(time.Second)
	w.work.mu.Lock()
	count := w.work.tickCount
	w.work.mu.Unlock()
	// nolint:errcheck
	_, _ = w.DefaultBaseWorker.MetaKVClient().Put(
		context.TODO(), tickKey, strconv.Itoa(count))

	w.work.mu.Lock()
	w.work.finished = true
	w.work.mu.Unlock()
}

func (w *exampleWorker) Init(ctx context.Context) error {
	log.L().Info("Init")

	if err := w.DefaultBaseWorker.Init(ctx); err != nil {
		return errors.Trace(err)
	}

	w.wg.Add(1)
	go w.run()
	return nil
}

func (w *exampleWorker) Poll(ctx context.Context) error {
	log.L().Info("Tick")
	w.work.mu.Lock()
	w.work.tickCount++
	w.work.mu.Unlock()
	return nil
}

func (w *exampleWorker) Status() lib.WorkerStatus {
	log.L().Info("Status")
	code := lib.WorkerStatusNormal
	w.work.mu.Lock()
	finished := w.work.finished
	w.work.mu.Unlock()

	if finished {
		code = lib.WorkerStatusFinished
	}
	return lib.WorkerStatus{Code: code}
}

func (w *exampleWorker) GetWorkerStatusExtTypeInfo() interface{} {
	// dummy
	return &struct{}{}
}

func (w *exampleWorker) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("OnMasterFailover")
	return nil
}

func (w *exampleWorker) Close(ctx context.Context) error {
	if err := w.DefaultBaseWorker.Close(ctx); err != nil {
		log.L().Warn("failed to close BaseWorker", zap.Error(err))
	}

	log.L().Info("Close")
	w.wg.Wait()
	return nil
}
