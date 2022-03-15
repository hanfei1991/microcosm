package config

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/stretchr/testify/require"
)

func TestJobCfg(t *testing.T) {
	t.Parallel()

	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile("./job_template.yaml"))
	require.Equal(t, "test", jobCfg.Name)

	clone, err := jobCfg.Clone()
	require.NoError(t, err)
	require.EqualValues(t, clone, jobCfg)

	require.Error(t, jobCfg.DecodeFile("./job_not_exist.yaml"))
}

func TestTaskCfg(t *testing.T) {
	t.Parallel()
	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile("./job_template.yaml"))

	taskCfgs := jobCfg.ToTaskConfigs()
	for task, taskCfg := range taskCfgs {
		subTaskCfg := taskCfg.ToDMSubTaskCfg(task)
		expectCfg := &dmconfig.SubTaskConfig{}
		_, err := toml.DecodeFile(fmt.Sprintf("./dm_subtask_%d.toml", taskCfg.Upstreams[0].DBCfg.Port), expectCfg)
		require.NoError(t, err)
		require.EqualValues(t, subTaskCfg, expectCfg)
	}
}
