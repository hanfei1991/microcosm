package benchmark

import (
	"context"
	"errors"
	"sync"

	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/log"
	"go.uber.org/zap"
)

type Master struct {
	*Config
	job *model.Job

	resouceManager cluster.ResourceMgr
	client         cluster.ExecutorClient

	offExecutors chan model.ExecutorID

	mu           sync.Mutex
	execTasks    map[model.ExecutorID][]*model.Task
	runningTasks map[model.TaskID]*Task

	scheduleWaitingTasks chan scheduleGroup
}

// tasks in same group have to be scheduled in the same node.
type scheduleGroup []*Task

type TaskStatus int32

const (
	Running TaskStatus = iota
	Stopped
	Finished
)

type Task struct {
	*model.Task

	exec   model.ExecutorID
	status TaskStatus
}

func (m *Master) ID() model.JobID {
	return m.job.ID
}

func (m *Master) dispatch(tasks []*Task) error {
	arrangement := make(map[model.ExecutorID][]*model.Task)
	for _, task := range tasks {
		subjob, ok := arrangement[task.exec]
		if !ok {
			arrangement[task.exec] = []*model.Task{task.Task}
		} else {
			subjob = append(subjob, task.Task)
			arrangement[task.exec] = subjob
		}
	}
	// TODO: process the error cases.
	for execID, taskList := range arrangement {
		// construct sub job
		job := &model.Job{
			ID:    m.job.ID,
			Tasks: taskList,
		}
		reqPb := job.ToPB()
		log.L().Logger.Info("submit sub job", zap.Int32("exec id", int32(execID)), zap.String("req pb", reqPb.String()))
		request := &cluster.ExecutorRequest{
			Cmd: cluster.CmdSubmitSubJob,
			Req: reqPb,
		}
		resp, err := m.client.Send(context.Background(), execID, request)
		if err != nil {
			log.L().Logger.Info("Send meet error", zap.Error(err))
			return err
		}
		respPb := resp.Resp.(*pb.SubmitSubJobResponse)
		if len(respPb.Errors) != 0 {
			return errors.New(respPb.Errors[0].GetMsg())
		}
	}
	m.mu.Lock()
	for eid, taskList := range arrangement {
		originTasks, ok := m.execTasks[eid]
		if ok {
			originTasks = append(originTasks, taskList...)
			m.execTasks[eid] = originTasks
		} else {
			m.execTasks[eid] = taskList
		}
	}
	for _, t := range tasks {
		m.runningTasks[t.ID] = t
	}
	m.mu.Unlock()
	return nil
}

// TODO: Implement different allocate task logic.
func (m *Master) allocateTasksWithNaitiveStratgy(snapshot *cluster.ResourceSnapshot, taskInfos []*model.Task) (bool, []*Task) {
	var idx int = 0
	tasks := make([]*Task, 0, len(taskInfos))
	for _, task := range taskInfos {
		originalIdx := idx
		nTask := &Task{
			Task: task,
		}
		for {
			exec := snapshot.Executors[idx]
			used := exec.Used
			if exec.Reserved > used {
				used = exec.Reserved
			}
			rest := exec.Capacity - used
			if rest >= cluster.ResourceUsage(task.Cost) {
				nTask.exec = exec.ID
				exec.Reserved = exec.Reserved + cluster.ResourceUsage(task.Cost)
				break
			}
			idx = (idx + 1) % len(snapshot.Executors)
			if idx == originalIdx {
				return false, nil
			}
		}
		tasks = append(tasks, nTask)
	}
	return true, tasks
}

func (m *Master) reScheduleTask(group scheduleGroup) error {
	snapshot := m.resouceManager.GetResourceSnapshot()
	if len(snapshot.Executors) == 0 {
		return errors.New("resource not enough")
	}
	taskInfos := make([]*model.Task, 0, len(group))
	for _, t := range group {
		taskInfos = append(taskInfos, t.Task)
	}
	success, tasks := m.allocateTasksWithNaitiveStratgy(snapshot, taskInfos)
	if !success {
		return errors.New("resource not enough")
	}
	if err := m.dispatch(tasks); err != nil {
		return err
	}
	return nil
}

func (m *Master) scheduleJobImpl() error {
	if m.job == nil {
		return errors.New("not found job")
	}
	snapshot := m.resouceManager.GetResourceSnapshot()
	if len(snapshot.Executors) == 0 {
		return errors.New("resource not enough")
	}
	success, tasks := m.allocateTasksWithNaitiveStratgy(snapshot, m.job.Tasks)
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
	// TODO: keep the goroutines alive.
	go m.monitorExecutorOffline()
	go m.monitorSchedulingTasks()
}

func (m *Master) monitorSchedulingTasks() {
	for {
		select {
		case group := <-m.scheduleWaitingTasks:
			//for _, t := range group {
			//	curT := m.runningTasks[t.ID]
			//	if curT.exec != t.exec {
			//		// this task has been scheduled away.
			//		log.L().Logger.Info("cur task exec id is not same as reschedule one", zap.Int32("cur id", int32(curT.exec)), zap.Int32("id", int32(t.exec)))
			//		continue
			//	}
			//}

			//if t.status == Running {
			// cancel it
			//}

			log.L().Logger.Info("begin to reschedule task group")
			if err := m.reScheduleTask(group); err != nil {
				log.L().Logger.Error("cant reschedule task", zap.Error(err))
				// FIXME: this will cause deadlock problem
				m.scheduleWaitingTasks <- group
			}
		}
	}
}

func (m *Master) OfflineExecutor(id model.ExecutorID) {
	m.offExecutors <- id
	log.L().Logger.Info("executor is offlined", zap.Int32("eid", int32(id)))
}

func (m *Master) monitorExecutorOffline() {
	for {
		select {
		case execID := <-m.offExecutors:
			log.L().Logger.Info("executor is offlined", zap.Int32("eid", int32(execID)))
			m.mu.Lock()
			taskList, ok := m.execTasks[execID]
			if !ok {
				m.mu.Unlock()
				log.L().Logger.Info("executor has been removed, nothing todo", zap.Int32("id", int32(execID)))
				continue
			}
			delete(m.execTasks, execID)
			m.mu.Unlock()

			log.L().Logger.Info("task number is", zap.Int("task num", len(taskList)))

			var group scheduleGroup
			for _, task := range taskList {
				t, ok := m.runningTasks[task.ID]
				if !ok || t.exec != execID {
					log.L().Logger.Error("running task is not consistant with executor-task map")
					continue
				}
				t.status = Finished
				group = append(group, t)
			}
			m.scheduleWaitingTasks <- group
		}
	}
}
