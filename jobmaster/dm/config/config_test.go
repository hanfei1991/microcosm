package config

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/stretchr/testify/require"
)

const (
	job_template_path    = "./job_template.yaml"
	subtask_template_dir = "."
)

func TestJobCfg(t *testing.T) {
	t.Parallel()

	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(job_template_path))
	require.Equal(t, "test", jobCfg.Name)

	clone, err := jobCfg.Clone()
	require.NoError(t, err)
	require.EqualValues(t, clone, jobCfg)

	require.Error(t, jobCfg.DecodeFile("./job_not_exist.yaml"))
}

func TestTaskCfg(t *testing.T) {
	t.Parallel()
	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(job_template_path))

	taskCfgs := jobCfg.ToTaskConfigs()
	for _, taskCfg := range taskCfgs {
		subTaskCfg := taskCfg.ToDMSubTaskCfg()
		expectCfg := &dmconfig.SubTaskConfig{}
		_, err := toml.DecodeFile(fmt.Sprintf("%s/dm_subtask_%d.toml", subtask_template_dir, taskCfg.Upstreams[0].DBCfg.Port), expectCfg)
		require.NoError(t, err)
		require.EqualValues(t, subTaskCfg, expectCfg)
	}
}
