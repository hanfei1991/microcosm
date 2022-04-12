package metadata

import (
	"context"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

// TODO: use Stage in lib or move Stage to lib.
type TaskStage int

// These stages may updated in later pr.
const (
	StageInit TaskStage = iota
	StageRunning
	StagePaused
	StageFinished
	// UnScheduled means the task is not scheduled.
	// This usually happens when the worker is offline.
	StageUnscheduled
)

// Job represents the state of a job.
type Job struct {
	State

	// taskID -> task
	Tasks map[string]*Task
}

func NewJob(jobCfg *config.JobCfg) *Job {
	taskCfgs := jobCfg.ToTaskConfigs()
	job := &Job{
		Tasks: make(map[string]*Task, len(taskCfgs)),
	}

	for taskID, taskCfg := range taskCfgs {
		job.Tasks[taskID] = NewTask(taskCfg)
	}
	return job
}

// A job may contain multiple upstream and it will be converted into multiple tasks.
type Task struct {
	Cfg   *config.TaskCfg
	Stage TaskStage
}

func NewTask(taskCfg *config.TaskCfg) *Task {
	return &Task{
		Cfg:   taskCfg,
		Stage: StageRunning, // TODO: support set stage when create task.
	}
}

// JobStore manages the state of a job.
type JobStore struct {
	*DefaultStore

	id libModel.MasterID
}

func NewJobStore(id libModel.MasterID, kvClient metaclient.KVClient) *JobStore {
	jobStore := &JobStore{
		DefaultStore: NewDefaultStore(kvClient),
		id:           id,
	}
	jobStore.DefaultStore.Store = jobStore
	return jobStore
}

func (jobStore *JobStore) CreateState() State {
	return &Job{}
}

func (jobStore *JobStore) Key() string {
	return adapter.DMJobKeyAdapter.Encode(jobStore.id)
}

// UpdateStages will be called if user operate job.
func (jobStore *JobStore) UpdateStages(ctx context.Context, taskIDs []string, stage TaskStage) error {
	state, err := jobStore.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	job := state.(*Job)
	for _, taskID := range taskIDs {
		if _, ok := job.Tasks[taskID]; !ok {
			return errors.Errorf("task %s not found", taskID)
		}
		t := job.Tasks[taskID]
		t.Stage = stage
	}

	return jobStore.Put(ctx, job)
}
