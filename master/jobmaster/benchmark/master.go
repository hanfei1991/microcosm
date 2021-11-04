package benchmark

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/model"
)

type Master struct {
	*Config
	job *model.Job

	resouceManager cluster.ResourceMgr
	client cluster.ExecutorClient
}

var lagThredshold int = 10

type TaskStatus int

const (
	Initing TaskStatus = iota
	Running 
	Disconnect 
	Dead 
	Finished 
)

type Task struct {
	sync.Mutex
	*model.Task

	lag int
	lastScheduleTime time.Time
}

func (m *Master) buildJob() error {
}

func (m *Master) dispatch(tasks []*model.Task) error {
	arrangement := make(map[model.ExecutorID][]*model.Task)
	for _, task := range tasks{
		subjob , ok := arrangement[task.ExecutorID]
		if !ok {
			arrangement[task.ExecutorID] = []*model.Task{task}
		} else {
			subjob = append(subjob, task)
		}
	}
	for id, taskList := range arrangement {
		// construct sub job
	}
	return  nil
}

func (m *Master) launch(arrangement map[model.ExecutorID][]*model.Task) error {
}

// TODO: Implement different allocate task logic.
func (m *Master) allocateTasksWithNaitiveStratgy(snapshot *cluster.ResourceSnapshot) (bool, []*model.Task) {
	var idx int = 0
	for _, task := range m.job.Tasks {
		originalIdx = idx
		for {
			exec := snapshot.Executors[idx]
			rest := int32(exec.Capacity) - math.MaxInt32(int32(exec.Usage), int32(exec.Reserved))
			if rest >= task.Cost {
				task.ExecutorID = exec.ID
				break
			}
			idx = (idx + 1) % len(snapshot.Executors)
			if idx == originalIdx {
				return false, nil
			}
		}
	}
	return true, m.job.Tasks
}

func (m *Master) scheduleJobImpl() (error) {
	if m.job == nil {
		return errors.New("not found job")
	}
	snapshot := m.resouceManager.GetResourceSnapshot()
	success, tasks := m.allocateTasksWithNaitiveStratgy(snapshot)
	if !success {
		return errors.New("resource not enough")
	}
	if err := m.dispatch(tasks); err != nil {
		return  err
	}

	m.start() // go
	return nil
}

func (m *Master) ScheduleJob() error {
	retry := 5;
	for i:=1; i <= retry; i++ {
		if  err := m.scheduleJobImpl(); err == nil {
			return nil
		} 		
		// sleep for a while to backoff
	}
	return nil
}

// Listen the events from every tasks
func (m *Master) start() {
	// Register Listen Handler to Msg Servers

	// Launch task

	// Run watch goroutines

}
