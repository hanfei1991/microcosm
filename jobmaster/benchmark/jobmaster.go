package benchmark

import (
	"context"

	"github.com/hanfei1991/microcosm/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/metadata"
)

type jobMaster struct {
	*system.Master
	config *Config

	stage1 []*model.Task
	stage2 []*model.Task
}

// TODO: Shall we pass an argument to indicate whether to recover from etcd?
func (m *jobMaster) Start(ctx context.Context, metaKV metadata.MetaKV) error {
	m.MetaKV = metaKV
	for _, task := range m.stage1 {
		if err := m.RestoreTask(ctx, task); err != nil {
			return err
		}
	}
	for _, task := range m.stage2 {
		if err := m.RestoreTask(ctx, task); err != nil {
			return err
		}
	}
	m.StartInternal(ctx)
	// TODO: Start the tasks manager to communicate.
	return nil
}

func (m *jobMaster) Stop(ctx context.Context) error {
	err := m.StopTasks(ctx, m.stage2)
	if err != nil {
		return err
	}
	err = m.StopTasks(ctx, m.stage1)
	if err != nil {
		return err
	}
	m.Cancel()
	return nil
}
