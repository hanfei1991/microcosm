package benchmark

import (
	"context"

	"github.com/hanfei1991/microcosm/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
)

type jobMasterDemo struct {
	*system.Master
	config *Config

	stage1 []*model.Task
	stage2 []*model.Task
}

func (m *jobMasterDemo) Start(ctx context.Context) error {
	m.StartInternal()
	// start stage1
	err := m.DispatchTasks(ctx, m.stage1)
	// start stage2
	if err != nil {
		return err
	}
	err = m.DispatchTasks(ctx, m.stage2)
	if err != nil {
		return err
	}
	// TODO: Start the tasks manager to communicate.
	return nil
}
