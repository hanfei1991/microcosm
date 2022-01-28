package example

import (
	"context"

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

	workerID     lib.WorkerID
	workerHandle lib.WorkerHandle
	tickCount    int
	receivedErr  error
}

func (e *exampleMaster) InitImpl(ctx context.Context) (err error) {
	log.L().Info("InitImpl")
	e.workerID, err = e.CreateWorker(exampleWorkerType, exampleWorkerCfg, exampleWorkerCost)
	return
}

func (e *exampleMaster) Tick(ctx context.Context) error {
	log.L().Info("Tick")
	e.tickCount++
	return nil
}

func (e *exampleMaster) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("OnMasterRecovered")
	return nil
}

func (e *exampleMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	log.L().Info("OnWorkerDispatched")
	e.workerHandle = worker
	e.receivedErr = result
	return nil
}

func (e *exampleMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("OnWorkerOnline")
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
