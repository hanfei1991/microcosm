package lib

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/executor/worker"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/errctx"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// BaseJobMaster defines an interface that can workr as a job master, it embeds
// a Worker interface which can run on dateflow engine runtime, and also provides
// some utility methods.
type BaseJobMaster interface {
	Worker

	OnError(err error)
	MetaKVClient() metaclient.KVClient
	GetWorkers() map[libModel.WorkerID]WorkerHandle
	CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit, resources ...resourcemeta.ResourceID) (libModel.WorkerID, error)
	JobMasterID() libModel.MasterID
	UpdateJobStatus(ctx context.Context, status libModel.WorkerStatus) error
	CurrentEpoch() libModel.Epoch

	// Exit should be called when job master (in user logic) wants to exit
	// - If err is nil, it means job master exits normally
	// - If err is not nil, it means job master meets error, and after it exits
	//   it will be failover.
	Exit(ctx context.Context, status libModel.WorkerStatus, err error) error

	// IsMasterReady returns whether the master has received heartbeats for all
	// workers after a fail-over. If this is the first time the JobMaster started up,
	// the return value is always true.
	IsMasterReady() bool

	// IsBaseJobMaster is an empty function used to prevent accidental implementation
	// of this interface.
	IsBaseJobMaster()
}

// DefaultBaseJobMaster implements BaseJobMaster interface
type DefaultBaseJobMaster struct {
	master    *DefaultBaseMaster
	worker    *DefaultBaseWorker
	impl      JobMasterImpl
	errCenter *errctx.ErrCenter
}

// JobMasterImpl is the implementation of a job master of dataflow engine.
// the implementation struct must embed the lib.BaseJobMaster interface, this
// interface will be initialized by the framework.
type JobMasterImpl interface {
	MasterImpl

	Workload() model.RescUnit
	OnJobManagerFailover(reason MasterFailoverReason) error
	OnJobManagerMessage(topic p2p.Topic, message interface{}) error
	// IsJobMasterImpl is an empty function used to prevent accidental implementation
	// of this interface.
	IsJobMasterImpl()
}

// NewBaseJobMaster creates a new DefaultBaseJobMaster instance
func NewBaseJobMaster(
	ctx *dcontext.Context,
	jobMasterImpl JobMasterImpl,
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
) BaseJobMaster {
	// master-worker pair: job manager <-> job master(`baseWorker` following)
	// master-worker pair: job master(`baseMaster` following) <-> real workers
	// `masterID` is always the ID of master role, against current object
	// `workerID` is the ID of current object
	baseMaster := NewBaseMaster(
		ctx, &jobMasterImplAsMasterImpl{jobMasterImpl}, workerID)
	baseWorker := NewBaseWorker(
		// TODO: need worker_type
		ctx, &jobMasterImplAsWorkerImpl{jobMasterImpl}, workerID, masterID)
	errCenter := errctx.NewErrCenter()
	baseMaster.(*DefaultBaseMaster).errCenter = errCenter
	baseWorker.(*DefaultBaseWorker).errCenter = errCenter
	return &DefaultBaseJobMaster{
		master:    baseMaster.(*DefaultBaseMaster),
		worker:    baseWorker.(*DefaultBaseWorker),
		impl:      jobMasterImpl,
		errCenter: errCenter,
	}
}

// MetaKVClient implements BaseJobMaster.MetaKVClient
func (d *DefaultBaseJobMaster) MetaKVClient() metaclient.KVClient {
	return d.master.MetaKVClient()
}

// Init implements BaseJobMaster.Init
func (d *DefaultBaseJobMaster) Init(ctx context.Context) error {
	ctx = d.errCenter.WithCancelOnFirstError(ctx)

	if err := d.worker.doPreInit(ctx); err != nil {
		return errors.Trace(err)
	}

	isFirstStartUp, err := d.master.doInit(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if isFirstStartUp {
		if err := d.impl.InitImpl(ctx); err != nil {
			return errors.Trace(err)
		}
		if err := d.master.markStatusCodeInMetadata(ctx, libModel.MasterStatusInit); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err := d.impl.OnMasterRecovered(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if err := d.worker.doPostInit(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Poll implements BaseJobMaster.Poll
func (d *DefaultBaseJobMaster) Poll(ctx context.Context) error {
	ctx = d.errCenter.WithCancelOnFirstError(ctx)

	if err := d.master.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := d.worker.doPoll(ctx); err != nil {
		if derror.ErrWorkerHalfExit.NotEqual(err) {
			return errors.Trace(err)
		}
		return nil
	}
	if err := d.impl.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetWorkers implements BaseJobMaster.GetWorkers
func (d *DefaultBaseJobMaster) GetWorkers() map[libModel.WorkerID]WorkerHandle {
	return d.master.GetWorkers()
}

// Close implements BaseJobMaster.Close
func (d *DefaultBaseJobMaster) Close(ctx context.Context) error {
	if err := d.impl.CloseImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	d.master.doClose()
	d.worker.doClose()
	return nil
}

// OnError implements BaseJobMaster.OnError
func (d *DefaultBaseJobMaster) OnError(err error) {
	// TODO refine the OnError logic.
	d.master.OnError(err)
}

// CreateWorker implements BaseJobMaster.CreateWorker
func (d *DefaultBaseJobMaster) CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit, resources ...resourcemeta.ResourceID) (libModel.WorkerID, error) {
	return d.master.CreateWorker(workerType, config, cost, resources...)
}

// UpdateStatus delegates the UpdateStatus of inner worker
func (d *DefaultBaseJobMaster) UpdateStatus(ctx context.Context, status libModel.WorkerStatus) error {
	ctx = d.errCenter.WithCancelOnFirstError(ctx)

	return d.worker.UpdateStatus(ctx, status)
}

// Workload delegates the Workload of inner worker
func (d *DefaultBaseJobMaster) Workload() model.RescUnit {
	return d.worker.Workload()
}

// ID delegates the ID of inner worker
func (d *DefaultBaseJobMaster) ID() worker.RunnableID {
	return d.worker.ID()
}

// JobMasterID delegates the JobMasterID of inner worker
func (d *DefaultBaseJobMaster) JobMasterID() libModel.MasterID {
	return d.master.MasterID()
}

// UpdateJobStatus implements BaseJobMaster.UpdateJobStatus
func (d *DefaultBaseJobMaster) UpdateJobStatus(ctx context.Context, status libModel.WorkerStatus) error {
	ctx = d.errCenter.WithCancelOnFirstError(ctx)

	return d.worker.UpdateStatus(ctx, status)
}

// CurrentEpoch implements BaseJobMaster.CurrentEpoch
func (d *DefaultBaseJobMaster) CurrentEpoch() libModel.Epoch {
	return d.master.currentEpoch.Load()
}

// IsBaseJobMaster implements BaseJobMaster.IsBaseJobMaster
func (d *DefaultBaseJobMaster) IsBaseJobMaster() {
}

// SendMessage delegates the SendMessage or inner worker
func (d *DefaultBaseJobMaster) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) (bool, error) {
	ctx = d.errCenter.WithCancelOnFirstError(ctx)

	// master will use WorkerHandle to send message
	return d.worker.SendMessage(ctx, topic, message)
}

// IsMasterReady implements BaseJobMaster.IsMasterReady
func (d *DefaultBaseJobMaster) IsMasterReady() bool {
	return d.master.IsMasterReady()
}

// Exit implements BaseJobMaster.Exit
func (d *DefaultBaseJobMaster) Exit(ctx context.Context, status libModel.WorkerStatus, err error) error {
	ctx = d.errCenter.WithCancelOnFirstError(ctx)

	var err1 error
	switch status.Code {
	case libModel.WorkerStatusFinished:
		err1 = d.master.markStatusCodeInMetadata(ctx, libModel.MasterStatusFinished)
	case libModel.WorkerStatusStopped:
		err1 = d.master.markStatusCodeInMetadata(ctx, libModel.MasterStatusStopped)
	}
	if err1 != nil {
		return err1
	}

	return d.worker.Exit(ctx, status, err)
}

type jobMasterImplAsWorkerImpl struct {
	inner JobMasterImpl
}

func (j *jobMasterImplAsWorkerImpl) InitImpl(ctx context.Context) error {
	log.L().Panic("unexpected Init call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) Tick(ctx context.Context) error {
	log.L().Panic("unexpected Poll call")
	return nil
}

func (j *jobMasterImplAsWorkerImpl) Workload() model.RescUnit {
	return j.inner.Workload()
}

func (j *jobMasterImplAsWorkerImpl) OnMasterFailover(reason MasterFailoverReason) error {
	return j.inner.OnJobManagerFailover(reason)
}

func (j *jobMasterImplAsWorkerImpl) OnMasterMessage(topic p2p.Topic, message interface{}) error {
	return j.inner.OnJobManagerMessage(topic, message)
}

func (j *jobMasterImplAsWorkerImpl) CloseImpl(ctx context.Context) error {
	log.L().Panic("unexpected Close call")
	return nil
}

type jobMasterImplAsMasterImpl struct {
	inner JobMasterImpl
}

func (j *jobMasterImplAsMasterImpl) OnWorkerStatusUpdated(worker WorkerHandle, newStatus *libModel.WorkerStatus) error {
	return j.inner.OnWorkerStatusUpdated(worker, newStatus)
}

func (j *jobMasterImplAsMasterImpl) Tick(ctx context.Context) error {
	log.L().Panic("unexpected poll call")
	return nil
}

func (j *jobMasterImplAsMasterImpl) InitImpl(ctx context.Context) error {
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

func (j *jobMasterImplAsMasterImpl) CloseImpl(ctx context.Context) error {
	log.L().Panic("unexpected Close call")
	return nil
}
