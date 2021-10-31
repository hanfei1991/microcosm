package benchmark

import (
	"errors"
	"fmt"

	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/model"
)

type Master struct {
	*Config
	job *model.Job

	resouceManager cluster.ResourceMgr

	client cluster.ExecutorClient
}

func (m *Master) buildJob() error {

}

func (m *Master) dispatch(arrangement map[model.ExecutorID][]*model.Task) error {
	for id, taskList := range arrangement {
		// construct sub job
		err := m.client.Send(ctx, id, &cluster.ExecutorRequest{})
		if err != nil {
			return err
		}
	}
	return  nil
}

func (m *Master) launch(arrangement map[model.ExecutorID][]*model.Task) error {
	topic := fmt.Sprintf("launch job/%d", m.job.ID)
	for id, _ := range arrangement {
		retry, err := m.client.Send(ctx, id, nil) 
		if err != nil {
			return err
		}
	}
	return nil

}

// TODO: Implement different allocate task logic.
func (m *Master) allocateTasksWithNaitiveStratgy(snapshot *cluster.ResourceSnapshot) (bool, map[model.ExecutorID][]*model.Task) {

}

func (m *Master) scheduleJobImpl() (error) {
	if m.job == nil {
		return errors.New("not found job")
	}
	snapshot := m.resouceManager.GetResourceSnapshot()
	success, arrangement := m.allocateTasksWithNaitiveStratgy(snapshot)
	if !success {
		return errors.New("resource not enough")
	}
	if err := m.dispatch(arrangement); err != nil {
		return  err
	}
	m.resouceManager.ApplyNewTasks(arrangement)
	m.start() // go
	return nil
}

func (m *Master) RescheduleTask(task *model.Task) (model.ExecutorID, error) {
	snapshot := m.resouceManager.GetResourceSnapshot()
	// find an executor
}

func (m *Master) ScheduleJob() error {
	retry := 5;
	for i:=1; i <= retry; i++ {
		if  err := m.scheduleJobImpl(); err == nil {
			return nil
		} 		
		// sleep for a while to backoff
	}
}

// Listen the events from every tasks
func (m *Master) start() {
	// Register Listen Handler to Msg Servers

	// Run watch goroutines
}