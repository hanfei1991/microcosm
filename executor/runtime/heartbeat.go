package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

const (
	noExec           model.ExecutorID = ""
	heartbeatTimeout time.Duration    = 8 * time.Second
	handshakeTimeout time.Duration    = 3 * time.Second
)

type taskWrapper struct {
	sync.Mutex
	stopped      bool // This task is timeout and closing.
	listenExec   model.ExecutorID
	task         *taskContainer
	registerTime time.Time
}

type execWrapper struct {
	sync.Mutex
	stopped          bool // This executor has lost heartbeat
	lastResponseTime time.Time
	taskList         map[model.ID]*taskWrapper
	id               model.ExecutorID
	removeSelf       func()
	removeTask       func(*taskWrapper)
}

type HeartbeatManager struct {
	sync.RWMutex
	taskList       map[model.ID]*taskWrapper
	executorGroups map[model.ExecutorID]*execWrapper
	s              *Runtime
}

func (h *HeartbeatManager) removeTask(e *execWrapper, task *taskWrapper) {
	task.Lock()
	if task.listenExec == e.id {
		// Still belong to this job master. stop it and clean
		// TODO: But sometimes we can just pause it and await recovery.
		task.stopped = true
		// we don't want task be locked for too long time.
		task.Unlock()
		err := h.s.Stop([]int64{int64(task.task.id)})
		log.L().Logger.Info("Stop task meed error", zap.Int64("id", int64(task.task.id)), zap.Error(err))
		h.Lock()
		delete(h.taskList, task.task.id)
		h.Unlock()
	} else {
		task.Unlock()
	}
}

// the executor has lost heartbeat so we removed it.
func (h *HeartbeatManager) removeExec(e *execWrapper) {
	h.Lock()
	delete(h.executorGroups, e.id)
	h.Unlock()
	e.Lock()
	defer e.Unlock()
	e.stopped = true
	for _, task := range e.taskList {
		e.removeTask(task)
	}
}

func (h *HeartbeatManager) Register(tasks ...*taskContainer) {
	noExecWrapper := h.executorGroups[noExec]
	for _, task := range tasks {
		wrapper := &taskWrapper{
			listenExec: noExec,
			task:       task,
		}
		h.Lock()
		h.taskList[task.id] = wrapper
		h.Unlock()
		noExecWrapper.Lock()
		noExecWrapper.taskList[task.id] = wrapper
		noExecWrapper.Unlock()
	}
}

func (e *execWrapper) check() {
	for {
		if !e.checkImpl() {
			return
		}
		// TODO: We don't need to check too frequently.
		time.Sleep(time.Second)
	}
}

func (e *execWrapper) checkImpl() bool {
	e.Lock()
	defer e.Unlock()
	if e.id == noExec {
		// This is a special exec wrapper. We should stop the tasks
		// that never be probed.
		removedTasks := make([]model.ID, 0)
		for _, task := range e.taskList {
			if task.registerTime.Add(handshakeTimeout).Before(time.Now()) {
				e.removeTask(task)
				removedTasks = append(removedTasks, task.task.id)
			}
		}
		for _, id := range removedTasks {
			delete(e.taskList, id)
		}
		return true
	}
	if e.lastResponseTime.Add(heartbeatTimeout).Before(time.Now()) {
		e.removeSelf()
		return false
	}
	return true
}

func (h *HeartbeatManager) Heartbeat(ctx context.Context, req *pb.ExecutorHeartbeatRequest) (*pb.ExecutorHeartbeatResponse, error) {
	exec := model.ExecutorID(req.ExecId)
	h.RLock()
	wrapper, ok := h.executorGroups[exec]
	noExecWrapper := h.executorGroups[noExec]
	h.RUnlock()
	if !ok {
		wrapper = &execWrapper{
			lastResponseTime: time.Now(),
		}
		wrapper.removeSelf = func() { h.removeExec(wrapper) }
		wrapper.removeTask = func(t *taskWrapper) { h.removeTask(wrapper, t) }
		go wrapper.check()
		h.Lock()
		h.executorGroups[exec] = wrapper
		h.Unlock()
	} else {
		// TODO if this executor is closing, we should replace it with a new one.
		wrapper.Lock()
		wrapper.lastResponseTime = time.Now()
		wrapper.Unlock()
	}
	resp := new(pb.ExecutorHeartbeatResponse)
	// add new tasks
	for _, rawID := range req.RegisterTaskIdList {
		id := model.ID(rawID)
		h.RLock()
		task, ok := h.taskList[id]
		h.RUnlock()
		if !ok {
			// this task not found
			resp.TaskStatus = append(resp.TaskStatus, &pb.TaskStatus{
				Id:     rawID,
				Status: pb.TaskStatusType_NotFound,
			})
		} else {
			task.Lock()
			if task.stopped {
				task.Unlock()
				resp.TaskStatus = append(resp.TaskStatus, &pb.TaskStatus{
					Id:     rawID,
					Status: pb.TaskStatusType_NotFound,
				})
				continue
			}
			task.listenExec = exec
			task.Unlock()
			wrapper.Lock()
			wrapper.taskList[task.task.id] = task
			wrapper.Unlock()
		}
	}

	removedTasks := make([]model.ID, 0, len(req.UnregisterTaskIdList))
	for _, rawID := range req.UnregisterTaskIdList {
		id := model.ID(rawID)
		h.Lock()
		if taskWrapper, ok := h.taskList[id]; ok {
			h.Unlock()
			taskWrapper.Lock()
			if taskWrapper.listenExec == exec {
				taskWrapper.listenExec = noExec
				taskWrapper.registerTime = time.Now()
				taskWrapper.Unlock()
				noExecWrapper.Lock()
				noExecWrapper.taskList[id] = taskWrapper
				noExecWrapper.Unlock()
			} else {
				taskWrapper.Unlock()
			}
		} else {
			h.Unlock()
		}
		removedTasks = append(removedTasks, id)
	}

	wrapper.Lock()
	for _, task := range wrapper.taskList {
		task.Lock()
		if task.listenExec != wrapper.id {
			removedTasks = append(removedTasks, task.task.id)
		}
		task.Unlock()
		status := &pb.TaskStatus{}
		switch TaskStatus(atomic.LoadInt32(&task.task.status)) {
		case Runnable, Waking:
			status.Status = pb.TaskStatusType_Running
		case Paused:
			status.Status = pb.TaskStatusType_Pause
		case Stop:
			status.Status = pb.TaskStatusType_Finished
		case Blocked:
			status.Status = pb.TaskStatusType_Blocked
		default:
			status.Status = pb.TaskStatusType_Err
			status.Err = &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: fmt.Sprintf("Unknown status %d", atomic.LoadInt32(&task.task.status)),
			}
		}
		resp.TaskStatus = append(resp.TaskStatus, status)
	}
	for _, id := range removedTasks {
		delete(wrapper.taskList, id)
	}
	wrapper.Unlock()
	return resp, nil
}
