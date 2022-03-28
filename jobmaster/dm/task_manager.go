package dm

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

var (
	// CheckInterval is the interval to check task status.
	CheckInterval = time.Second * 30
	// ErrorInterval is the interval to check task status when error occurs.
	// TODO: add backoff strategy
	ErrorInterval = time.Second * 10
)

// TODO: use OperateType in lib or move OperateType to lib.
type OperateType int

// These op may updated in later pr.
// NOTICE: consider to only use Update cmd to add/remove task.
// e.g. start-task/stop-task -s source in origin DM will be replaced by update-job now.
const (
	Pause OperateType = iota
	Resume
	Update
	Delete
)

type Agent interface {
	OperateTask(ctx context.Context, taskID string, stage metadata.TaskStage) error
}

// TaskManager checks and operates task.
type TaskManager struct {
	jobStore    *metadata.JobStore
	workerAgent Agent
	triggerCh   chan struct{}
	// tasks record the runtime task status
	// taskID -> TaskStatus
	tasks sync.Map
}

func NewTaskManager(initTaskStatus []runtime.TaskStatus, jobStore *metadata.JobStore, agent Agent) *TaskManager {
	taskManager := &TaskManager{
		jobStore:    jobStore,
		triggerCh:   make(chan struct{}, 1),
		workerAgent: agent,
	}
	for _, taskStatus := range initTaskStatus {
		taskManager.UpdateTaskStatus(taskStatus)
	}
	return taskManager
}

// OperateTask is called by user request.
func (tm *TaskManager) OperateTask(ctx context.Context, op OperateType, jobCfg *config.JobCfg, tasks []string) (err error) {
	log.L().Info("operate task", zap.Int("op", int(op)), zap.Strings("tasks", tasks))
	defer func() {
		if err == nil {
			tm.Trigger(ctx, 0)
		}
	}()

	var stage metadata.TaskStage
	switch op {
	case Delete:
		return tm.jobStore.Delete(ctx)
	case Update:
		return tm.jobStore.Put(ctx, metadata.NewJob(jobCfg))
	case Resume:
		stage = metadata.StageRunning
	case Pause:
		stage = metadata.StagePaused
	default:
		return errors.New("unknown operate type")
	}

	return tm.jobStore.UpdateStages(ctx, tasks, stage)
}

// UpdateTaskStatus is called when receive task status from worker.
func (tm *TaskManager) UpdateTaskStatus(taskStatus runtime.TaskStatus) {
	log.L().Debug("update task status", zap.String("task_id", taskStatus.GetTask()), zap.Int("stage", int(taskStatus.GetStage())), zap.Int("unit", int(taskStatus.GetUnit())))
	tm.tasks.Store(taskStatus.GetTask(), taskStatus)
}

// TaskStatus return the task status.
func (tm *TaskManager) TaskStatus() map[string]runtime.TaskStatus {
	result := make(map[string]runtime.TaskStatus)
	tm.tasks.Range(func(key, value interface{}) bool {
		result[key.(string)] = value.(runtime.TaskStatus)
		return true
	})
	return result
}

// Trigger triggers the task manager to check and operate task.
// TODO: Implement a more accurate and efficient way to trigger task manager if needed.
func (tm *TaskManager) Trigger(ctx context.Context, delay time.Duration) {
	triggerFunc := func() {
		select {
		case <-ctx.Done():
			return
		case tm.triggerCh <- struct{}{}:
			log.L().Info("trigger task manager")
		default:
			log.L().Info("task manager is already triggered")
		}
	}

	if delay > 0 {
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
				triggerFunc()
			}
		}()
	} else {
		triggerFunc()
	}
}

// Run checks and operates task.
func (tm *TaskManager) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.L().Info("exit task manager run")
			return
		case <-tm.triggerCh:
			log.L().Info("check task status by trigger")
		case <-time.After(CheckInterval):
			log.L().Info("check task status by interval")
		}

		state, err := tm.jobStore.Get(ctx)
		if err != nil {
			log.L().Error("get job state failed", zap.Error(err))
			tm.onJobNotExist(ctx)
			continue
		}
		job := state.(*metadata.Job)

		tm.checkAndOperateTasks(ctx, job)
		tm.removeTaskStatus(job)
	}
}

func (tm *TaskManager) checkAndOperateTasks(ctx context.Context, job *metadata.Job) {
	var runningTask runtime.TaskStatus

	// check and operate task
	for taskID, persistentTask := range job.Tasks {
		task, ok := tm.tasks.Load(taskID)
		if ok {
			runningTask = task.(runtime.TaskStatus)
		}

		// task unbounded or worker offline
		if !ok || runningTask.GetStage() == metadata.StageUnscheduled {
			log.L().Error("get task status failed", zap.String("task_id", taskID))
			tm.Trigger(ctx, ErrorInterval)
			continue
		}

		if taskAsExpected(persistentTask, runningTask) {
			log.L().Debug("task status as expected", zap.String("task_id", taskID), zap.Int("stage", int(runningTask.GetStage())))
			continue
		}

		log.L().Info("unexpected task status", zap.String("task_id", taskID), zap.Int("expected_stage", int(persistentTask.Stage)), zap.Int("stage", int(runningTask.GetStage())))
		// OperateTask should be a asynchronous request
		if err := tm.workerAgent.OperateTask(ctx, taskID, persistentTask.Stage); err != nil {
			log.L().Error("operate task failed", zap.Error(err))
			tm.Trigger(ctx, ErrorInterval)
			continue
		}
	}
}

// remove all tasks, usually happened when delete jobs.
func (tm *TaskManager) onJobNotExist(ctx context.Context) {
	log.L().Info("clear all task status")
	tm.tasks.Range(func(key, value interface{}) bool {
		tm.tasks.Delete(key)
		return true
	})
	tm.Trigger(ctx, ErrorInterval)
}

// remove deleted task status, usually happened when update-job delete some tasks.
func (tm *TaskManager) removeTaskStatus(job *metadata.Job) {
	tm.tasks.Range(func(key, value interface{}) bool {
		taskID := key.(string)
		if _, ok := job.Tasks[taskID]; !ok {
			log.L().Info("remove task status", zap.String("task_id", taskID))
			tm.tasks.Delete(taskID)
		}
		return true
	})
}

// check a task runs as expected.
func taskAsExpected(persistentTask *metadata.Task, taskStatus runtime.TaskStatus) bool {
	// TODO: when running is expected but task is paused, we may still need return true,
	// because worker will resume it automatically.
	return persistentTask.Stage == taskStatus.GetStage()
}
