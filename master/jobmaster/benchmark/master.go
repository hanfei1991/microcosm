package benchmark

import (
	"errors"
	"fmt"

	"github.com/hanfei1991/microcosom/master"
	"github.com/hanfei1991/microcosom/model"
)

type Master struct {
	*Config
	job *model.Job

	resouceManager master.ResourceMgr

	messageClient  MessageClient
	messageServer  MessageServer
}

func (m *Master) buildJob() error {

}

func (m *Master) dispatch(arrangement map[model.ExecutorID][]*model.Task) (bool, error) {
	topic := "dispatch job"
	for id, taskList := range arrangement {
		subJob := model.Job {
			ID: m.job.ID,
			Tasks: taskList,
		}
		retry, err := m.messageClient.SendMessage(id, topic, subJob.ToPB())
		if err != nil {
			return retry, err
		}
	}
	return false, nil
}

func (m *Master) launch(arrangement map[model.ExecutorID][]*model.Task) (bool, error) {
	topic := fmt.Sprintf("launch job/%d", m.job.ID)
	for id, _ := range arrangement {
		retry, err := m.messageClient.SendMessage(id, topic, nil) 
		if err != nil {
			return retry, err
		}
	}
	return false, nil

}

// TODO: Implement different allocate task logic.
func (m *Master) allocateTasksWithNaitiveStratgy(snapshot *scheduler.ResourceSnapshot) (bool, map[model.ExecutorID][]*model.Task) {

}

func (m *Master) scheduleJobImpl() (bool, error) {
	if m.job == nil {
		return false, errors.New("not found job")
	}
	snapshot := m.resouceManager.GetResourceSnapshot()
	success, arrangement := m.allocateTasksWithNaitiveStratgy(snapshot)
	if !success {
		return true, errors.New("resource not enough")
	}
	if retry, err := m.dispatch(arrangement); err != nil {
		return retry, err
	}
	m.start()
	if retry, err := m.launch(arrangement); err != nil {
		return retry, err
	}
	m.resouceManager.ApplyNewTasks(arrangement)
	return false, nil
}

func (m *Master) RescheduleTask(txn *ReschedulerTxn) (model.ExecutorID, error) {
	snapshot := m.resouceManager.GetResourceSnapshot()
	// find a least
}

func (m *Master) ScheduleJob() error {
	retry := 5;
	for i:=1; i <= retry; i++ {
		if retry, err := m.scheduleJobImpl(); err == nil {
			return nil
		} else if !retry {
			return err
		}
		// sleep for a while to backoff
	}
}

// Listen the events from every tasks
func (m *Master) start() {
	// Register Listen Handler to Msg Servers

	// Run watch goroutines
}