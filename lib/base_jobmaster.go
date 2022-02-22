package lib

import (
	"context"

	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
)

type BaseJobMaster interface {
	Init(ctx context.Context) (isFirstStartUp bool, err error)
	Poll(ctx context.Context) error
	Close(ctx context.Context) error
	OnError(err error)

	MetaKVClient() metadata.MetaKV

	GetWorkers() map[WorkerID]WorkerHandle

	RegisterWorker(ctx context.Context, workerID WorkerID) error
	CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error)

	GetWorkerStatusExtTypeInfo() interface{}
	GetJobMasterStatusExtTypeInfo() interface{}

	Workload() model.RescUnit

	JobMasterID() MasterID
	ID() worker.RunnableID
	UpdateJobStatus(ctx context.Context, status WorkerStatus) error

	// IsBaseJobMaster is an empty function used to prevent accidental implementation
	// of this interface.
	IsBaseJobMaster()
}

type defaultBaseJobMaster struct {
	master BaseMaster
	worker BaseWorker
}

type JobMasterImpl interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	Close(ctx context.Context) error

	OnMasterRecovered(ctx context.Context) error
	OnWorkerDispatched(worker WorkerHandle, result error) error
	OnWorkerOnline(worker WorkerHandle) error
	OnWorkerOffline(worker WorkerHandle, reason error) error
	OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error
	GetWorkerStatusExtTypeInfo() interface{}
	GetJobMasterStatusExtTypeInfo() interface{}

	Workload() model.RescUnit
	OnJobManagerFailover(reason MasterFailoverReason) error

	// IsJobMasterImpl is an empty function used to prevent accidental implementation
	// of this interface.
	IsJobMasterImpl()
}

func NewBaseJobMaster(
	ctx *dcontext.Context,
	jobMasterImpl JobMasterImpl,
	masterID MasterID,
	workerID WorkerID,
	messageHandlerManager p2p.MessageHandlerManager,
	messageRouter p2p.MessageSender,
	metaKVClient metadata.MetaKV,
	executorClientManager client.ClientsManager,
	serverMasterClient client.MasterClient,
) BaseJobMaster {
	// master-worker pair: job manager <-> job master(`baseWorker` following)
	// master-worker pair: job master(`baseMaster` following) <-> real workers
	// `masterID` is always the ID of master role, against current object
	// `workerID` is the ID of current object
	baseMaster := NewBaseMaster(
		ctx, &jobMasterImplAsMasterImpl{jobMasterImpl}, workerID, messageHandlerManager,
		messageRouter, metaKVClient, executorClientManager, serverMasterClient)
	baseWorker := NewBaseWorker(
		&jobMasterImplAsWorkerImpl{jobMasterImpl}, messageHandlerManager, messageRouter, metaKVClient,
		workerID, masterID)
	return &defaultBaseJobMaster{
		master: baseMaster,
		worker: baseWorker,
	}
}

func (d *defaultBaseJobMaster) MetaKVClient() metadata.MetaKV {
	return d.master.MetaKVClient()
}

func (d *defaultBaseJobMaster) Init(ctx context.Context) ( /* isFirstStartUp */ bool, error) {
	if err := d.worker.Init(ctx); err != nil {
		return false, errors.Trace(err)
	}

	isFirstStartUp, err := d.master.Init(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}

	return isFirstStartUp, nil
}

func (d *defaultBaseJobMaster) Poll(ctx context.Context) error {
	if err := d.worker.Poll(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := d.master.Poll(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *defaultBaseJobMaster) MasterID() MasterID {
	return d.master.MasterID()
}

func (d *defaultBaseJobMaster) GetWorkers() map[WorkerID]WorkerHandle {
	return d.master.GetWorkers()
}

func (d *defaultBaseJobMaster) Close(ctx context.Context) error {
	if err := d.master.Close(ctx); err != nil {
		// TODO should we close the worker anyways even if closing the master has failed?
		return errors.Trace(err)
	}
	if err := d.worker.Close(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *defaultBaseJobMaster) OnError(err error) {
	// TODO refine the OnError logic.
	d.master.OnError(err)
}

func (d *defaultBaseJobMaster) RegisterWorker(ctx context.Context, workerID WorkerID) error {
	return d.master.RegisterWorker(ctx, workerID)
}

func (d *defaultBaseJobMaster) CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error) {
	return d.master.CreateWorker(workerType, config, cost)
}

func (d *defaultBaseJobMaster) GetWorkerStatusExtTypeInfo() interface{} {
	return d.master.GetWorkerStatusExtTypeInfo()
}

func (d *defaultBaseJobMaster) UpdateStatus(ctx context.Context, status WorkerStatus) error {
	return d.worker.UpdateStatus(ctx, status)
}

func (d *defaultBaseJobMaster) Workload() model.RescUnit {
	return d.worker.Workload()
}

func (d *defaultBaseJobMaster) ID() worker.RunnableID {
	return d.worker.ID()
}

func (d *defaultBaseJobMaster) GetJobMasterStatusExtTypeInfo() interface{} {
	panic("implement me")
}

func (d *defaultBaseJobMaster) JobMasterID() MasterID {
	return d.master.MasterID()
}

func (d *defaultBaseJobMaster) UpdateJobStatus(ctx context.Context, status WorkerStatus) error {
	return d.worker.UpdateStatus(ctx, status)
}

func (d *defaultBaseJobMaster) IsBaseJobMaster() {
}

type jobMasterImplAsWorkerImpl struct {
	inner JobMasterImpl
}

func (j *jobMasterImplAsWorkerImpl) Init(ctx context.Context) error {
	log.L().Panic("unexpected Init call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) Poll(ctx context.Context) error {
	log.L().Panic("unexpected Poll call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) Workload() model.RescUnit {
	return j.inner.Workload()
}

func (j *jobMasterImplAsWorkerImpl) OnMasterFailover(reason MasterFailoverReason) error {
	return j.inner.OnJobManagerFailover(reason)
}

func (j *jobMasterImplAsWorkerImpl) Close(ctx context.Context) error {
	log.L().Panic("unexpected Close call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) GetWorkerStatusExtTypeInfo() interface{} {
	return j.inner.GetWorkerStatusExtTypeInfo()
}

type jobMasterImplAsMasterImpl struct {
	inner JobMasterImpl
}

func (j *jobMasterImplAsMasterImpl) Poll(ctx context.Context) error {
	log.L().Panic("unexpected poll call")
	return nil
}

func (j *jobMasterImplAsMasterImpl) Init(ctx context.Context) error {
	log.L().Panic("unexpected init call")
	return nil
}

func (j *jobMasterImplAsMasterImpl) OnMasterRecovered(ctx context.Context) error {
	return j.inner.OnMasterRecovered(ctx)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerDispatched(worker WorkerHandle, result error) error {
	return j.inner.OnWorkerDispatched(worker, result)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerOnline(worker WorkerHandle) error {
	return j.inner.OnWorkerOnline(worker)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerOffline(worker WorkerHandle, reason error) error {
	return j.inner.OnWorkerOffline(worker, reason)
}

func (j *jobMasterImplAsMasterImpl) OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error {
	return j.inner.OnWorkerMessage(worker, topic, message)
}

func (j *jobMasterImplAsMasterImpl) Close(ctx context.Context) error {
	log.L().Panic("unexpected Close call")
	return nil
}

func (j *jobMasterImplAsMasterImpl) GetWorkerStatusExtTypeInfo() interface{} {
	return j.inner.GetJobMasterStatusExtTypeInfo()
}
