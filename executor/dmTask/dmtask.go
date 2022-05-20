package dmtask

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hanfei1991/microcosm/jobmaster/dm"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterWorker is used to register dm task to global registry
func RegisterWorker() {
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMDump, newTaskFactory(lib.WorkerDMDump))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMLoad, newTaskFactory(lib.WorkerDMLoad))
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.WorkerDMSync, newTaskFactory(lib.WorkerDMSync))
}

// taskFactory create dm task
type taskFactory struct {
	workerType libModel.WorkerType
}

// newTaskFactory creates abstractFactory
func newTaskFactory(workerType libModel.WorkerType) *taskFactory {
	return &taskFactory{workerType: workerType}
}

// DeserializeConfig implements WorkerFactory.DeserializeConfig
func (f taskFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &dmconfig.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}

// NewWorkerImpl implements WorkerFactory.NewWorkerImpl
func (f taskFactory) NewWorkerImpl(ctx *dcontext.Context, workerID libModel.WorkerID, masterID libModel.MasterID, conf lib.WorkerConfig) (lib.WorkerImpl, error) {
	baseDMTask := NewBaseDMTask(ctx, f.workerType, conf)
	switch f.workerType {
	case lib.WorkerDMDump:
		return newDumpTask(baseDMTask), nil
	case lib.WorkerDMLoad:
		return newLoadTask(baseDMTask), nil
	default:
		return newSyncTask(baseDMTask), nil
	}
}

// DMTask defines the interface for dump/load/sync
type DMTask interface {
	onInit(ctx context.Context) error
	onFinished(ctx context.Context) error
	createUnitHolder(cfg *config.SubTaskConfig) *unitHolder
}

// BaseDMTask implements some default methods for dm task
type BaseDMTask struct {
	DMTask
	lib.BaseWorker
	unitHolder *unitHolder

	cfg                *dmconfig.SubTaskConfig
	storageWriteHandle broker.Handle
	stage              metadata.TaskStage
	workerType         libModel.WorkerType
	taskID             string
}

// NewBaseDMTask creates BaseDMTask instances
func NewBaseDMTask(ctx *dcontext.Context, workerType libModel.WorkerType, conf lib.WorkerConfig) BaseDMTask {
	return BaseDMTask{
		cfg:        conf.(*config.SubTaskConfig),
		stage:      metadata.StageInit,
		workerType: workerType,
		taskID:     conf.(*config.SubTaskConfig).SourceID,
	}
}

// InitImpl implements lib.BaseWorker.InitImpl
func (t *BaseDMTask) InitImpl(ctx context.Context) error {
	log.L().Info("init task")
	if err := t.onInit(ctx); err != nil {
		return err
	}
	t.unitHolder = t.createUnitHolder(t.cfg)
	return t.unitHolder.init(ctx)
}

// Tick implements lib.WorkerImpl.Tick
func (t *BaseDMTask) Tick(ctx context.Context) error {
	t.unitHolder.lazyProcess()
	if err := t.unitHolder.tick(ctx); err != nil {
		return err
	}
	return t.tryUpdateStatus(ctx)
}

// Workload implements lib.WorkerImpl.Worload
func (t *BaseDMTask) Workload() model.RescUnit {
	log.L().Info("dmtask.Workload")
	return 0
}

// OnMasterFailover implements lib.WorkerImpl.OnMasterFailover
func (t *BaseDMTask) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("dmtask.OnMasterFailover")
	return nil
}

// OnMasterMessage implements lib.WorkerImpl.OnMasterMessage
func (t *BaseDMTask) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("dmtask.OnMasterMessage", zap.Any("message", message))
	return nil
}

// CloseImpl implements lib.WorkerImpl.CloseImpl
func (t *BaseDMTask) CloseImpl(ctx context.Context) error {
	t.unitHolder.close()
	return nil
}

// setupStorge Open and configs external storage
func (t *BaseDMTask) setupStorge(ctx context.Context) error {
	rid := dm.NewDMResourceID(t.cfg.Name, t.cfg.SourceID)
	h, err := t.OpenStorage(ctx, rid)
	for status.Code(err) == codes.Unavailable {
		log.L().Info("simple retry", zap.Error(err))
		time.Sleep(time.Second)
		h, err = t.OpenStorage(ctx, rid)
	}
	if err != nil {
		return errors.Trace(err)
	}
	t.storageWriteHandle = h
	t.cfg.ExtStorage = h.BrExternalStorage()
	return nil
}

// persistStorge persist storge.
func (t *BaseDMTask) persistStorge(ctx context.Context) error {
	return t.storageWriteHandle.Persist(ctx)
}

func (t *BaseDMTask) tryUpdateStatus(ctx context.Context) error {
	stage := t.unitHolder.Stage()
	if stage == t.stage {
		return nil
	}
	log.L().Info("task stage changed", zap.Int("from", int(stage)), zap.Int("to", int(t.stage)))
	t.stage = stage

	status := t.workerStatus(stage)
	if stage != metadata.StageFinished {
		return t.UpdateStatus(ctx, status)
	}

	if err := t.onFinished(ctx); err != nil {
		log.L().Error("failed to handle finished status", zap.Error(err))
		// retry next tick
		return nil
	}
	return t.Exit(ctx, status, nil)
}

func (t *BaseDMTask) workerStatus(stage metadata.TaskStage) libModel.WorkerStatus {
	code := libModel.WorkerStatusNormal
	if stage == metadata.StageFinished {
		code = libModel.WorkerStatusFinished
	}
	status := runtime.DefaultTaskStatus{
		Unit:  t.workerType,
		Task:  t.taskID,
		Stage: stage,
	}
	// nolint:errcheck
	statusBytes, _ := json.Marshal(status)
	return libModel.WorkerStatus{
		Code:     code,
		ExtBytes: statusBytes,
	}
}
