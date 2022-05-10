package config

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/stretchr/testify/require"
)

const (
	jobTemplatePath    = "./job_template.yaml"
	subtaskTemplateDir = "."
)

func TestJobCfg(t *testing.T) {
	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	require.Equal(t, "test", jobCfg.Name)
	content, err := jobCfg.Yaml()
	require.NoError(t, err)

	clone, err := jobCfg.Clone()
	require.NoError(t, err)
	content2, err := clone.Yaml()
	require.NoError(t, err)
	require.Equal(t, content2, content)

	dmTaskCfg, err := clone.toDMTaskCfg()
	require.NoError(t, err)
	require.NoError(t, clone.fromDMTaskCfg(dmTaskCfg))
	content3, err := clone.Yaml()
	require.NoError(t, err)
	require.Equal(t, content3, content)

	require.Error(t, jobCfg.DecodeFile("./job_not_exist.yaml"))
}

func TestTaskCfg(t *testing.T) {
	jobCfg := &JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))

	taskCfgs := jobCfg.ToTaskConfigs()
	for _, taskCfg := range taskCfgs {
		subTaskCfg := taskCfg.ToDMSubTaskCfg()
		expectCfg := &dmconfig.SubTaskConfig{}
		_, err := toml.DecodeFile(fmt.Sprintf("%s/dm_subtask_%d.toml", subtaskTemplateDir, taskCfg.Upstreams[0].DBCfg.Port), expectCfg)
		require.NoError(t, err)
		require.EqualValues(t, subTaskCfg, expectCfg)
	}
}
