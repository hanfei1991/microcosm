package metadata

import (
	"context"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/pingcap/errors"
)

// TODO: use Stage in lib or move Stage to lib.
type TaskStage int

// These stages may updated in later pr.
const (
	StageInit TaskStage = iota
	StageRunning
	StagePaused
	StageFinished
	StageDelete
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

	for source, taskCfg := range taskCfgs {
		job.Tasks[source] = NewTask(taskCfg)
	}
	return job
}

// Task represents the status of a upstream task.
type Task struct {
	Cfg   *config.TaskCfg
	Stage TaskStage
}

func NewTask(taskCfg *config.TaskCfg) *Task {
	return &Task{
		Cfg:   taskCfg,
		Stage: StageInit,
	}
}

// JobStore manages the state of a job.
type JobStore struct {
	DefaultStore

	id lib.MasterID
}

func NewJobStore(id lib.MasterID, kvClient metadata.MetaKV) *JobStore {
	jobStore := &JobStore{
		DefaultStore: *NewDefaultStore(kvClient),
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

// Operate will be called if user operate job.
func (jobStore *JobStore) Operate(ctx context.Context, taskIDs []string, stage TaskStage) error {
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
