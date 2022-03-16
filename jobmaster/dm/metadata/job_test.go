package metadata

import (
	"context"
	"testing"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	meta "github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/stretchr/testify/require"
)

func TestJobStore(t *testing.T) {
	var (
		source1 = "mysql-replica-01"
		source2 = "mysql-replica-02"
	)

	t.Parallel()

	jobStore := NewJobStore("job_test", meta.NewMetaMock())
	key := jobStore.Key()
	keys, err := adapter.DMJobKeyAdapter.Decode(key)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], "job_test")

	require.Error(t, jobStore.Operate(context.Background(), []string{}, StageRunning))

	state := jobStore.CreateState()
	require.IsType(t, &Job{}, state)

	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile("../config/job_template.yaml"))

	job := NewJob(jobCfg)
	require.NoError(t, jobStore.Put(context.Background(), job))
	state, err = jobStore.Get(context.Background())
	require.NoError(t, err)
	require.NotNil(t, state)
	require.IsType(t, &Job{}, state)

	job = state.(*Job)
	require.Len(t, job.Tasks, len(jobCfg.Upstreams))
	require.Contains(t, job.Tasks, source1)
	require.Contains(t, job.Tasks, source1)
	require.Equal(t, job.Tasks[source1].Stage, StageInit)
	require.Equal(t, job.Tasks[source2].Stage, StageInit)

	require.Error(t, jobStore.Operate(context.Background(), []string{"task-not-exist"}, StageRunning))
	require.Error(t, jobStore.Operate(context.Background(), []string{source1, "task-not-exist"}, StageRunning))
	state, _ = jobStore.Get(context.Background())
	job = state.(*Job)
	require.Equal(t, job.Tasks[source1].Stage, StageInit)
	require.Equal(t, job.Tasks[source2].Stage, StageInit)

	require.NoError(t, jobStore.Operate(context.Background(), []string{source1, source2}, StageRunning))
	require.Equal(t, job.Tasks[source1].Stage, StageInit)
	require.Equal(t, job.Tasks[source2].Stage, StageInit)
	state, _ = jobStore.Get(context.Background())
	job = state.(*Job)
	require.Equal(t, job.Tasks[source1].Stage, StageRunning)
	require.Equal(t, job.Tasks[source2].Stage, StageRunning)

	require.NoError(t, jobStore.Operate(context.Background(), []string{source2}, StageFinished))
	state, _ = jobStore.Get(context.Background())
	job = state.(*Job)
	require.Equal(t, job.Tasks[source1].Stage, StageRunning)
	require.Equal(t, job.Tasks[source2].Stage, StageFinished)
}
