package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

const (
	// homelessGroup has yet been watched by any executors
	none             model.ExecutorID = ""
	heartbeatTimeout time.Duration    = 8 * time.Second
)

type taskWrapper struct {
	sync.Mutex
	*taskContainer

	stopped      bool // This task is timeout and closing.
	listenExec   model.ExecutorID
	registerTime time.Time
}

// taskGroup is a struct to react the heartbeat request.
type taskGroup struct {
	sync.Mutex
	stopped          bool // This executor has lost heartbeat
	lastResponseTime time.Time
	taskList         map[model.ID]*taskWrapper
	id               model.ExecutorID
	removeSelf       func()
	removeTask       func(*taskWrapper)
}

func (h *HeartbeatServer) newTaskGroup(id model.ExecutorID) *taskGroup {
	tg := &taskGroup{
		id:         id,
		taskList:   make(map[model.ID]*taskWrapper),
		removeTask: h.removeTask,
	}
	tg.removeSelf = func() { h.removeGroup(tg) }
	return tg
}

type homelessGroup struct {
	*taskGroup

	newOrphans chan *taskWrapper // use channel to accept orphans, in order to avoid silly lock.
}

func (h *homelessGroup) check() {
	for {
		h.checkImpl()
		time.Sleep(time.Second)
	}
}

func (h *homelessGroup) checkImpl() {
	// welcome new homeless tasks!
	for {
		select {
		case task := <-h.newOrphans:
			h.taskList[task.id] = task
		default:
			goto ABANDON
		}
	}
ABANDON: // See who have lost connection or have been stopped. Kick them out!
	abandonedTasks := make([]model.ID, 0)
	moveoutTasks := make([]model.ID, 0)
	for _, task := range h.taskList {
		task.Lock()
		if task.listenExec != none {
			// These tasks no longer living in the orphanage. Congrats!
			moveoutTasks = append(moveoutTasks, task.id)
		}
		// Right now there are no difference between handshake timeout and heartbeat timeout.
		// We don't know whether a task is a new comer or a veteran from other groups.
		// For the task has been stopped, we clean them to avoid leak.
		if task.registerTime.Add(heartbeatTimeout).Before(time.Now()) || task.GetStatus() == Stop {
			h.removeTask(task)
			abandonedTasks = append(abandonedTasks, task.id)
		}
		task.Unlock()
	}
	for _, id := range abandonedTasks {
		h.removeTask(h.taskList[id])
		delete(h.taskList, id)
	}
	for _, id := range moveoutTasks {
		delete(h.taskList, id)
	}
}

type HeartbeatServer struct {
	taskList   sync.Map       // map[model.ID]*taskWrapper // hold all tasks
	taskGroups sync.Map       // map[model.ExecutorID]*taskGroup // one group corresponds for one executor
	orphanage  *homelessGroup // poor tasks who're not watched by anyone.
	s          *Runtime
}

func NewHeartbeatServer(s *Runtime) *HeartbeatServer {
	hb := &HeartbeatServer{
		orphanage: &homelessGroup{
			newOrphans: make(chan *taskWrapper, 1024),
		},
		s: s,
	}
	hb.orphanage.taskGroup = hb.newTaskGroup(none)
	go hb.orphanage.check()
	return hb
}

func (h *HeartbeatServer) removeTask(task *taskWrapper) {
	task.Lock()
	// TODO: But sometimes we can just pause it and await recovery.
	task.stopped = true
	// we don't want task be locked for too long time.
	task.Unlock()

	//
	err := h.s.Stop([]int64{int64(task.id)})
	log.L().Logger.Info("Stop task meed error", zap.Int64("id", int64(task.id)), zap.Error(err))
	h.taskList.Delete(task.id)
}

// There are multiple places trying to stop and to remove a task.
// To avoid complexity, we move the task to orphanage to dispose together.
func (h *HeartbeatServer) moveToOrphanage(t *taskWrapper) {
	t.Lock()
	t.listenExec = none
	t.registerTime = time.Now()
	t.Unlock()
	h.orphanage.newOrphans <- t
}

// the executor has lost heartbeat so we removed it.
func (h *HeartbeatServer) removeGroup(e *taskGroup) {
	// step one, kick out the group
	h.taskGroups.Delete(e.id)
	// step two, kick every task out.
	e.Lock()
	defer e.Unlock()
	e.stopped = true
	for _, task := range e.taskList {
		h.moveToOrphanage(task)
	}
}

// Register the tasks, at first they are all homeless
func (h *HeartbeatServer) Register(tasks ...*taskContainer) {
	for _, task := range tasks {
		wrapper := &taskWrapper{
			listenExec:    none,
			taskContainer: task,
		}

		h.taskList.Store(task.id, wrapper)
		h.moveToOrphanage(wrapper)
	}
}

func (e *taskGroup) check() {
	for {
		if !e.checkImpl() {
			return
		}
		// TODO: We don't need to check too frequently.
		time.Sleep(time.Second)
	}
}

func (e *taskGroup) checkImpl() bool {
	e.Lock()
	if e.lastResponseTime.Add(heartbeatTimeout).Before(time.Now()) {
		e.Unlock()
		e.removeSelf()
		return false
	}
	e.Unlock()
	return true
}

func (h *HeartbeatServer) Heartbeat(ctx context.Context, req *pb.ExecutorHeartbeatRequest) (*pb.ExecutorHeartbeatResponse, error) {
	if req.TargetExecId != string(client.SelfExecID) {
		return &pb.ExecutorHeartbeatResponse{
			Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownExecutor,
				Message: errors.ErrUnknownExecutorID.FastGenByArgs(req.TargetExecId).Error(),
			},
		}, nil
	}

	exec := model.ExecutorID(req.SourceExecId)
	value, ok := h.taskGroups.Load(exec)
	group := value.(*taskGroup)

	if !ok {
		group = h.newTaskGroup(exec)
		go group.check()
		h.taskGroups.Store(exec, group)
	}

	// group should hold this lock util quit!
	group.Lock()
	defer group.Unlock()

	if group.stopped {
		// This group is on the way to self destructed
		return &pb.ExecutorHeartbeatResponse{
			Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: "the heartbeat has been lost, please reconnect!",
			},
		}, nil
	}

	group.lastResponseTime = time.Now()
	resp := new(pb.ExecutorHeartbeatResponse)
	// add new tasks
	for _, rawID := range req.RegisterTaskIdList {
		id := model.ID(rawID)
		value, ok := h.taskList.Load(id)
		if !ok {
			// this task not found
			resp.TaskStatus = append(resp.TaskStatus, &pb.TaskStatus{
				Id:     rawID,
				Status: pb.TaskStatusType_NotFound,
			})
		} else {
			task := value.(*taskWrapper)
			task.Lock()
			if task.stopped {
				task.Unlock()
				resp.TaskStatus = append(resp.TaskStatus, &pb.TaskStatus{
					Id:     rawID,
					Status: pb.TaskStatusType_NotFound,
				})
				continue
			}
			// We simply change the listen exec, but don't have to remove it from the other side.
			task.listenExec = exec
			task.Unlock()
			group.taskList[task.id] = task
		}
	}

	removedTasks := make([]model.ID, 0, len(req.UnregisterTaskIdList))
	for _, rawID := range req.UnregisterTaskIdList {
		id := model.ID(rawID)
		if value, ok := h.taskList.Load(id); ok {
			taskWrapper := value.(*taskWrapper)
			taskWrapper.Lock()
			if taskWrapper.listenExec == exec {
				taskWrapper.Unlock()
				h.moveToOrphanage(taskWrapper)
			} else {
				// This task has been listened by another executor before this executor gives up watching.
				// This is not supposed to happen. Maybe split brain has been happen.
				taskWrapper.Unlock()
			}
		}
		removedTasks = append(removedTasks, id)
	}

	// TODO: We don't have to sweep all the tasks
	for _, task := range group.taskList {
		task.Lock()
		if task.listenExec != group.id {
			// This task has been moved to another place.
			// so we can remove it safely.
			removedTasks = append(removedTasks, task.id)
		}
		task.Unlock()
		status := &pb.TaskStatus{}
		switch TaskStatus(atomic.LoadInt32(&task.status)) {
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
				Message: fmt.Sprintf("Unknown status %d", atomic.LoadInt32(&task.status)),
			}
		}
		resp.TaskStatus = append(resp.TaskStatus, status)
	}
	for _, id := range removedTasks {
		delete(group.taskList, id)
	}
	return resp, nil
}
