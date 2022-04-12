package dm

import (
	"context"
	"sync"
	"time"

	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/jobmaster/dm/ticker"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
)

var (
	WorkerNormalInterval = time.Second * 30
	WorkerErrorInterval  = time.Second * 10
)

type WorkerAgent interface {
	CreateWorker(ctx context.Context, taskID string, workerType libModel.WorkerType, taskCfg *config.TaskCfg) (libModel.WorkerID, error)
	DestroyWorker(ctx context.Context, taskID string, workerID libModel.WorkerID) error
}

type CheckpointAgent interface {
	IsFresh(ctx context.Context, workerType libModel.WorkerType, taskCfg *metadata.Task) (bool, error)
}

type WorkerManager struct {
	*ticker.DefaultTicker

	jobStore        *metadata.JobStore
	workerAgent     WorkerAgent
	checkpointAgent CheckpointAgent

	// workerStatusMap record the runtime worker status
	// taskID -> WorkerStatus
	workerStatusMap sync.Map
}

// WorkerManager checks and schedules workers.
func NewWorkerManager(initWorkerStatus []runtime.WorkerStatus, jobStore *metadata.JobStore, workerAgent WorkerAgent, checkpointAgent CheckpointAgent) *WorkerManager {
	workerManager := &WorkerManager{
		DefaultTicker:   ticker.NewDefaultTicker(WorkerNormalInterval, WorkerErrorInterval),
		jobStore:        jobStore,
		workerAgent:     workerAgent,
		checkpointAgent: checkpointAgent,
	}
	workerManager.DefaultTicker.Ticker = workerManager

	for _, workerStatus := range initWorkerStatus {
		workerManager.UpdateWorkerStatus(workerStatus)
	}
	return workerManager
}

// UpdateWorkerStatus is called when receive worker status.
func (wm *WorkerManager) UpdateWorkerStatus(workerStatus runtime.WorkerStatus) {
	log.L().Debug("update worker status", zap.String("task_id", workerStatus.TaskID), zap.String("worker_id", workerStatus.ID))
	wm.workerStatusMap.Store(workerStatus.TaskID, workerStatus)
}

// WorkerStatus return the worker status.
func (wm *WorkerManager) WorkerStatus() map[string]runtime.WorkerStatus {
	result := make(map[string]runtime.WorkerStatus)
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.(runtime.WorkerStatus)
		return true
	})
	return result
}

// TickImpl remove offline workers.
// TickImpl destroy unneeded workers.
// TickImpl create new workers if needed.
func (wm *WorkerManager) TickImpl(ctx context.Context) error {
	log.L().Info("start to schedule workers")
	wm.removeOfflineWorkers()

	state, err := wm.jobStore.Get(ctx)
	if err != nil {
		log.L().Error("get job state failed", zap.Error(err))
		if err2 := wm.onJobNotExist(ctx); err2 != nil {
			return err2
		}
		return err
	}
	job := state.(*metadata.Job)

	var recordError error
	if err := wm.destroyUnneededWorkers(ctx, job); err != nil {
		recordError = err
	}
	if err := wm.checkAndScheduleWorkers(ctx, job); err != nil {
		recordError = err
	}
	return recordError
}

// remove offline worker status, usually happened when worker is offline.
func (wm *WorkerManager) removeOfflineWorkers() {
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		worker := value.(runtime.WorkerStatus)
		if worker.IsOffline() {
			log.L().Info("remove offline worker status", zap.String("task_id", worker.TaskID))
			wm.workerStatusMap.Delete(key)
		}
		return true
	})
}

// destroy all workers, usually happened when delete jobs.
func (wm *WorkerManager) onJobNotExist(ctx context.Context) error {
	var recordError error
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		log.L().Info("destroy worker", zap.String("task_id", key.(string)), zap.String("worker_id", value.(runtime.WorkerStatus).ID))
		if err := wm.destroyWorker(ctx, key.(string), value.(runtime.WorkerStatus).ID); err != nil {
			recordError = err
		}
		return true
	})
	return recordError
}

// destroy unneeded workers, usually happened when update-job delete some tasks.
func (wm *WorkerManager) destroyUnneededWorkers(ctx context.Context, job *metadata.Job) error {
	var recordError error
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		if _, ok := job.Tasks[taskID]; !ok {
			log.L().Info("destroy unneeded worker", zap.String("task_id", taskID), zap.String("worker_id", value.(runtime.WorkerStatus).ID))
			if err := wm.destroyWorker(ctx, taskID, value.(runtime.WorkerStatus).ID); err != nil {
				recordError = err
			}
		}
		return true
	})
	return recordError
}

// checkAndScheduleWorkers check whether a task need a new worker.
// If there is no related worker, create a new worker.
// If task is finished, check whether need a new worker.
// This function does not handle taskCfg updated(update-job).
// TODO: support update taskCfg, or we may need to send update request manually.
func (wm *WorkerManager) checkAndScheduleWorkers(ctx context.Context, job *metadata.Job) error {
	var (
		runningWorker runtime.WorkerStatus
		nextUnit      libModel.WorkerType
		err           error
		recordError   error
	)

	// check and schedule workers
	for taskID, persistentTask := range job.Tasks {
		worker, ok := wm.workerStatusMap.Load(taskID)
		if ok {
			runningWorker = worker.(runtime.WorkerStatus)
			nextUnit = getNextUnit(persistentTask, runningWorker)
		} else if nextUnit, err = wm.getCurrentUnit(ctx, persistentTask); err != nil {
			log.L().Error("get current unit failed", zap.String("task", taskID), zap.Error(err))
			recordError = err
			continue
		}

		if ok && runningWorker.RunAsExpected() && nextUnit == runningWorker.Unit {
			log.L().Debug("worker status as expected", zap.String("task_id", taskID), zap.Int("worker_stage", int(runningWorker.Stage)), zap.Int64("unit", int64(runningWorker.Unit)))
			continue
		} else if !ok {
			log.L().Info("task has no worker", zap.String("task_id", taskID), zap.Int64("unit", int64(nextUnit)))
		} else if !runningWorker.RunAsExpected() {
			log.L().Info("unexpected worker status", zap.String("task_id", taskID), zap.Int("worker_stage", int(runningWorker.Stage)), zap.Int64("unit", int64(runningWorker.Unit)), zap.Int64("next_unit", int64(nextUnit)))
		} else {
			log.L().Info("switch to next unit", zap.String("task_id", taskID), zap.Int64("next_unit", int64(runningWorker.Unit)))
		}

		// createWorker should be a asynchronous operation
		if err := wm.createWorker(ctx, taskID, nextUnit, persistentTask.Cfg); err != nil {
			recordError = err
			continue
		}
	}
	return recordError
}

func (wm *WorkerManager) getCurrentUnit(ctx context.Context, task *metadata.Task) (libModel.WorkerType, error) {
	var workerSeq []libModel.WorkerType

	switch task.Cfg.TaskMode {
	case dmconfig.ModeAll:
		workerSeq = []libModel.WorkerType{
			lib.WorkerDMDump,
			lib.WorkerDMLoad,
			lib.WorkerDMSync,
		}
	case dmconfig.ModeFull:
		workerSeq = []libModel.WorkerType{
			lib.WorkerDMDump,
			lib.WorkerDMLoad,
		}
	case dmconfig.ModeIncrement:
		return lib.WorkerDMSync, nil
	}

	for i := len(workerSeq) - 1; i >= 0; i-- {
		isFresh, err := wm.checkpointAgent.IsFresh(ctx, workerSeq[i], task)
		if err != nil {
			return 0, err
		}
		if !isFresh {
			return workerSeq[i], nil
		}
	}

	return workerSeq[0], nil
}

func getNextUnit(task *metadata.Task, worker runtime.WorkerStatus) libModel.WorkerType {
	if worker.Stage != runtime.WorkerFinished {
		return worker.Unit
	}

	if worker.Unit == lib.WorkerDMDump || task.Cfg.TaskMode == dmconfig.ModeFull {
		return lib.WorkerDMLoad
	}
	return lib.WorkerDMSync
}

func (wm *WorkerManager) createWorker(ctx context.Context, taskID string, unit libModel.WorkerType, taskCfg *config.TaskCfg) error {
	workerID, err := wm.workerAgent.CreateWorker(ctx, taskID, unit, taskCfg)
	if err != nil {
		log.L().Error("failed to create workers", zap.String("task_id", taskID), zap.Int64("unit", int64(unit)), zap.Error(err))
	}
	if len(workerID) != 0 {
		//	There are two mechanisms for create workers status.
		//	1.	create worker status when no error.
		//		It is possible that the worker will be created twice, so the create needs to be idempotent.
		//	2.	create worker status even if there is error.
		//		When create fails, we create it again until the next time we receive WorkerDispatchFailed/WorkerOffline event, so the create interval will be longer.
		//	We need to handle the intermediate state.
		//	We choose the second mechanism now.
		//	Disscuss: Is there a case where a worker is created but never receives a dispatch/online/offline event?
		//	Dissucss: If master crash before dispatch/online, will the worker be created twice?
		wm.UpdateWorkerStatus(runtime.NewWorkerStatus(taskID, unit, workerID, runtime.WorkerCreating))
	}
	return err
}

func (wm *WorkerManager) destroyWorker(ctx context.Context, taskID string, workerID libModel.WorkerID) error {
	if err := wm.workerAgent.DestroyWorker(ctx, taskID, workerID); err != nil {
		log.L().Error("failed to destroy worker", zap.String("task_id", taskID), zap.String("worker_id", workerID), zap.Error(err))
		return err
	}
	//	There are two mechanisms for removing worker status.
	//	1.	remove worker status when no error.
	//		It is possible that the worker will be destroyed twice, so the destroy needs to be idempotent.
	//	2.	remove worker status even if there is error.
	//		When destroy fails, we destroy it again until the next time we receive worker online status, so the destroy interval will be longer.
	//	We choose the first mechanism now.
	wm.workerStatusMap.Delete(taskID)
	return nil
}

func (wm *WorkerManager) removeWorkerStatusByWorkerID(workerID libModel.WorkerID) {
	wm.workerStatusMap.Range(func(key, value interface{}) bool {
		if value.(runtime.WorkerStatus).ID == workerID {
			wm.workerStatusMap.Delete(key)
			return false
		}
		return true
	})
}
