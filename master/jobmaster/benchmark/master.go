package benchmark

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
)

type Master struct {
	*Config
	job *model.Job

	resouceManager cluster.ResourceMgr
	client         cluster.ExecutorClient
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

	lag              int
	lastScheduleTime time.Time
}

func (m *Master) ID() model.JobID {
	return m.job.ID
}

func (m *Master) dispatch(tasks []*model.Task) error {
	arrangement := make(map[model.ExecutorID][]*model.Task)
	for _, task := range tasks {
		subjob, ok := arrangement[task.ExecutorID]
		if !ok {
			arrangement[task.ExecutorID] = []*model.Task{task}
		} else {
			subjob = append(subjob, task)
		}
	}
	// TODO: process the error cases.
	for execID, taskList := range arrangement {
		// construct sub job
		job := &model.Job{
			ID: m.job.ID,
			Tasks: taskList,
		}
		reqPb := job.ToPB()
		request := &cluster.ExecutorRequest{
			Cmd: cluster.CmdSubmitSubJob,
			Req: reqPb,
		}
		resp, err := m.client.Send(context.Background(), execID, request)
		if err != nil {
			return err
		}
		respPb := resp.Resp.(*pb.SubmitJobResponse)
		if respPb.ErrMessage != "" {
			return errors.New(respPb.ErrMessage)
		}
	}
	return nil
}

// TODO: Implement different allocate task logic.
func (m *Master) allocateTasksWithNaitiveStratgy(snapshot *cluster.ResourceSnapshot) (bool, []*model.Task) {
	var idx int = 0
	for _, task := range m.job.Tasks {
		originalIdx := idx
		for {
			exec := snapshot.Executors[idx]
			used := exec.Used
			if exec.Reserved > used {
				used = exec.Reserved
			}
			rest := exec.Capacity - used
			if rest >= cluster.ResourceUsage(task.Cost) {
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

func (m *Master) scheduleJobImpl() error {
	if m.job == nil {
		return errors.New("not found job")
	}
	snapshot := m.resouceManager.GetResourceSnapshot()
	success, tasks := m.allocateTasksWithNaitiveStratgy(snapshot)
	if !success {
		return errors.New("resource not enough")
	}

	m.start() // go
	if err := m.dispatch(tasks); err != nil {
		return err
	}
	return nil
}

func (m *Master) DispatchJob() error {
	retry := 1
	for i := 1; i <= retry; i++ {
		if err := m.scheduleJobImpl(); err == nil {
			return nil
		} else if i == retry {
			return err
		}
		// sleep for a while to backoff
	}
	return nil
}

// Listen the events from every tasks
func (m *Master) start() {
	// Register Listen Handler to Msg Servers

	// Run watch goroutines

}
