package dm

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

const (
	jobTemplatePath = "./config/job_template.yaml"
)

func TestUpdateTaskStatus(t *testing.T) {
	t.Parallel()
	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("task_manager_test", mock.NewMetaMock())
	require.NoError(t, jobStore.Put(context.Background(), job))
	taskManager := NewTaskManager(nil, jobStore, nil)

	require.Len(t, taskManager.TaskStatus(), 0)

	dumpStatus1 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[0].SourceID,
			Stage: metadata.StageRunning,
		},
	}
	dumpStatus2 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[1].SourceID,
			Stage: metadata.StageRunning,
		},
	}

	taskManager.UpdateTaskStatus(dumpStatus1)
	taskManager.UpdateTaskStatus(dumpStatus2)
	taskStatusMap := taskManager.TaskStatus()
	require.Len(t, taskStatusMap, 2)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[0].SourceID], dumpStatus1)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	loadStatus1 := &runtime.LoadStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  jobCfg.Upstreams[0].SourceID,
			Stage: metadata.StageRunning,
		},
	}
	taskManager.UpdateTaskStatus(loadStatus1)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t, taskStatusMap, 2)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	// offline
	offlineStatus := runtime.NewOfflineStatus(jobCfg.Upstreams[1].SourceID)
	taskManager.UpdateTaskStatus(offlineStatus)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t, taskStatusMap, 2)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[1].SourceID], offlineStatus)

	// online
	taskManager.UpdateTaskStatus(dumpStatus2)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t, taskStatusMap, 2)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)

	// mock jobmaster recover
	taskStatusList := make([]runtime.TaskStatus, 0, len(taskStatusMap))
	for _, taskStatus := range taskStatusMap {
		taskStatusList = append(taskStatusList, taskStatus)
	}
	taskManager = NewTaskManager(taskStatusList, jobStore, nil)
	taskStatusMap = taskManager.TaskStatus()
	require.Len(t, taskStatusMap, 2)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[0].SourceID)
	require.Contains(t, taskStatusMap, jobCfg.Upstreams[1].SourceID)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[0].SourceID], loadStatus1)
	require.Equal(t, taskStatusMap[jobCfg.Upstreams[1].SourceID], dumpStatus2)
}

func TestOperateTask(t *testing.T) {
	t.Parallel()
	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("task_manager_test", mock.NewMetaMock())
	require.NoError(t, jobStore.Put(context.Background(), job))
	taskManager := NewTaskManager(nil, jobStore, nil)

	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID

	state, err := jobStore.Get(context.Background())
	require.NoError(t, err)
	job = state.(*metadata.Job)
	require.Equal(t, job.Tasks[source1].Stage, metadata.StageRunning)
	require.Equal(t, job.Tasks[source2].Stage, metadata.StageRunning)

	taskManager.OperateTask(context.Background(), Pause, nil, []string{source1, source2})
	state, err = jobStore.Get(context.Background())
	require.NoError(t, err)
	job = state.(*metadata.Job)
	require.Equal(t, job.Tasks[source1].Stage, metadata.StagePaused)
	require.Equal(t, job.Tasks[source2].Stage, metadata.StagePaused)

	taskManager.OperateTask(context.Background(), Resume, nil, []string{source1})
	state, err = jobStore.Get(context.Background())
	require.NoError(t, err)
	job = state.(*metadata.Job)
	require.Equal(t, job.Tasks[source1].Stage, metadata.StageRunning)
	require.Equal(t, job.Tasks[source2].Stage, metadata.StagePaused)

	taskManager.OperateTask(context.Background(), Update, jobCfg, nil)
	state, err = jobStore.Get(context.Background())
	require.NoError(t, err)
	job = state.(*metadata.Job)
	require.Equal(t, job.Tasks[source1].Stage, metadata.StageRunning)
	// TODO: should it be paused?
	require.Equal(t, job.Tasks[source2].Stage, metadata.StageRunning)

	taskManager.OperateTask(context.Background(), Delete, nil, []string{source1, source2})
	state, err = jobStore.Get(context.Background())
	require.EqualError(t, err, "state not found")
}

func TestClearTaskStatus(t *testing.T) {
	taskManager := NewTaskManager(nil, nil, nil)
	syncStatus1 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  "source1",
			Stage: metadata.StageRunning,
		},
	}
	syncStatus2 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  "source2",
			Stage: metadata.StageRunning,
		},
	}

	taskManager.UpdateTaskStatus(syncStatus1)
	taskManager.UpdateTaskStatus(syncStatus2)
	require.Len(t, taskManager.TaskStatus(), 2)

	job := metadata.NewJob(&config.JobCfg{})
	job.Tasks["source1"] = metadata.NewTask(&config.TaskCfg{})

	taskManager.removeTaskStatus(job)
	require.Len(t, taskManager.TaskStatus(), 1)

	job.Tasks["source3"] = metadata.NewTask(&config.TaskCfg{})
	taskManager.removeTaskStatus(job)
	require.Len(t, taskManager.TaskStatus(), 1)

	taskManager.onJobNotExist(context.Background())
	require.Len(t, taskManager.TaskStatus(), 0)

	taskManager.onJobNotExist(context.Background())
	require.Len(t, taskManager.TaskStatus(), 0)
}

func TestTaskAsExpected(t *testing.T) {
	require.True(t, taskAsExpected(&metadata.Task{Stage: metadata.StagePaused},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StagePaused}}))
	require.True(t, taskAsExpected(&metadata.Task{Stage: metadata.StageRunning},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StageRunning}}))
	require.False(t, taskAsExpected(&metadata.Task{Stage: metadata.StagePaused},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StageRunning}}))

	// TODO: change below results if needed.
	require.False(t, taskAsExpected(&metadata.Task{Stage: metadata.StageRunning},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StagePaused}}))
	require.False(t, taskAsExpected(&metadata.Task{Stage: metadata.StageRunning},
		&runtime.DumpStatus{DefaultTaskStatus: runtime.DefaultTaskStatus{Stage: metadata.StageFinished}}))
}

func TestTaskManager(t *testing.T) {
	t.Parallel()
	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	job := metadata.NewJob(jobCfg)
	jobStore := metadata.NewJobStore("task_manager_test", mock.NewMetaMock())
	require.NoError(t, jobStore.Put(context.Background(), job))
	mockAgent := &MockAgent{}
	taskManager := NewTaskManager(nil, jobStore, mockAgent)
	source1 := jobCfg.Upstreams[0].SourceID
	source2 := jobCfg.Upstreams[1].SourceID

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	CheckInterval = time.Hour
	ErrorInterval = time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1)
	// run task manager
	go func() {
		defer wg.Done()
		taskManager.Run(ctx)
	}()

	// mock trigger when start
	taskManager.Trigger(ctx, 0)

	syncStatus1 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  source1,
			Stage: metadata.StageRunning,
		},
	}
	syncStatus2 := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMSync,
			Task:  source2,
			Stage: metadata.StageRunning,
		},
	}

	mockAgent.SetStages(map[string]metadata.TaskStage{source1: metadata.StageRunning, source2: metadata.StageRunning})
	// receive worker online
	taskManager.UpdateTaskStatus(syncStatus1)
	taskManager.UpdateTaskStatus(syncStatus2)

	// task1 paused unexpectedly
	agentError := errors.New("agent error")
	mockAgent.SetResult([]error{agentError, agentError, agentError, nil})
	syncStatus1.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(syncStatus1)

	// mock check by interval
	taskManager.Trigger(ctx, time.Second)
	// resumed eventually
	require.Eventually(t, func() bool {
		mockAgent.Lock()
		defer mockAgent.Unlock()
		return len(mockAgent.results) == 0 && mockAgent.stages[source1] == metadata.StageRunning
	}, 5*time.Second, 100*time.Millisecond)
	syncStatus1.Stage = metadata.StageRunning
	taskManager.UpdateTaskStatus(syncStatus1)

	// manually pause task2
	taskManager.OperateTask(ctx, Pause, nil, []string{source2})
	mockAgent.SetResult([]error{agentError, agentError, agentError, nil})
	// paused eventually
	require.Eventually(t, func() bool {
		mockAgent.Lock()
		defer mockAgent.Unlock()
		return len(mockAgent.results) == 0 && mockAgent.stages[source2] == metadata.StagePaused
	}, 5*time.Second, 100*time.Millisecond)
	syncStatus2.Stage = metadata.StagePaused
	taskManager.UpdateTaskStatus(syncStatus2)

	// task2 offline
	taskManager.UpdateTaskStatus(runtime.NewOfflineStatus(source2))
	// mock check by interval
	taskManager.Trigger(ctx, time.Millisecond)
	// no request, no panic in mockAgent
	time.Sleep(1 * time.Second)

	// task2 online
	taskManager.UpdateTaskStatus(syncStatus2)

	// mock remove task2 by update-job
	jobCfg.Upstreams = jobCfg.Upstreams[:1]
	taskManager.OperateTask(ctx, Update, jobCfg, nil)
	require.Eventually(t, func() bool {
		mockAgent.Lock()
		defer mockAgent.Unlock()
		return len(mockAgent.results) == 0 && len(taskManager.TaskStatus()) == 1
	}, 5*time.Second, 100*time.Millisecond)

	// mock delete job
	taskManager.OperateTask(ctx, Delete, nil, nil)

	cancel()
	wg.Wait()
}

type MockAgent struct {
	sync.Mutex
	results []error
	stages  map[string]metadata.TaskStage
}

func (mockAgent *MockAgent) SetResult(results []error) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.results = append(mockAgent.results, results...)
}

func (mockAgent *MockAgent) SetStages(stages map[string]metadata.TaskStage) {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	mockAgent.stages = stages
}

func (mockAgent *MockAgent) OperateTask(ctx context.Context, taskID string, stage metadata.TaskStage) error {
	mockAgent.Lock()
	defer mockAgent.Unlock()
	if len(mockAgent.results) == 0 {
		panic("no result in mock agent")
	}
	mockAgent.stages[taskID] = stage
	result := mockAgent.results[0]
	mockAgent.results = mockAgent.results[1:]
	return result
}
