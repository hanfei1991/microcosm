package example

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

const (
	exampleWorkerType = 999
	exampleWorkerCfg  = "config"
	exampleWorkerCost = 100
)

var _ lib.Master = &exampleMaster{}

type exampleMaster struct {
	*lib.BaseMaster

	worker struct {
		mu sync.Mutex

		id         lib.WorkerID
		handle     lib.WorkerHandle
		online     bool
		statusCode lib.WorkerStatusCode
	}

	tickCount   int
	receivedErr error
}

func (e *exampleMaster) InitImpl(ctx context.Context) (err error) {
	log.L().Info("InitImpl")
	e.worker.mu.Lock()
	e.worker.id, err = e.CreateWorker(exampleWorkerType, exampleWorkerCfg, exampleWorkerCost)
	e.worker.mu.Unlock()
	return
}

func (e *exampleMaster) Tick(ctx context.Context) error {
	log.L().Info("Tick")
	e.tickCount++

	e.worker.mu.Lock()
	defer e.worker.mu.Unlock()
	handle := e.worker.handle
	if handle == nil {
		return nil
	}
	e.worker.statusCode = handle.Status().Code
	return nil
}

func (e *exampleMaster) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("OnMasterRecovered")
	return nil
}

func (e *exampleMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	log.L().Info("OnWorkerDispatched")
	e.worker.mu.Lock()
	e.worker.handle = worker
	e.worker.mu.Unlock()
	e.receivedErr = result
	return nil
}

func (e *exampleMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("OnWorkerOnline")
	e.worker.mu.Lock()
	e.worker.online = true
	e.worker.mu.Unlock()
	return nil
}

func (e *exampleMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("OnWorkerOffline")
	return nil
}

func (e *exampleMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("OnWorkerMessage")
	return nil
}

func (e *exampleMaster) CloseImpl(ctx context.Context) error {
	log.L().Info("CloseImpl")
	return nil
}
