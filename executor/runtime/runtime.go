package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/test"
)

type queue struct {
	sync.Mutex
	tasks []*taskContainer
}

func (q *queue) pop() *taskContainer {
	q.Lock()
	defer q.Unlock()
	if len(q.tasks) == 0 {
		return nil
	}
	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	return task
}

func (q *queue) push(t *taskContainer) {
	q.Lock()
	defer q.Unlock()
	q.tasks = append(q.tasks, t)
}

type Runtime struct {
	testCtx   *test.Context
	tasksLock sync.RWMutex
	tasks     map[model.ID]*taskContainer
	q         queue
	wg        sync.WaitGroup
	hb        *HeartbeatServer
}

func (s *Runtime) HandleHeartbeat(ctx context.Context, req *pb.ExecutorHeartbeatRequest) (*pb.ExecutorHeartbeatResponse, error) {
	return s.hb.Heartbeat(ctx, req)
}

// Resource returns current usage resource snapshot of this runtime
func (s *Runtime) Resource() map[model.WorkloadType]model.RescUnit {
	s.tasksLock.RLock()
	defer s.tasksLock.RUnlock()
	res := make(map[model.WorkloadType]model.RescUnit)
	for _, t := range s.tasks {
		res[t.tru.GetType()] += t.tru.GetUsage()
	}
	return res
}

func (s *Runtime) Stop(tasks []int64) error {
	s.tasksLock.Lock()
	defer s.tasksLock.Unlock()
	var retErr error
	for _, id := range tasks {
		if task, ok := s.tasks[model.ID(id)]; ok {
			err := task.Stop()
			if err != nil {
				retErr = err
			}
			delete(s.tasks, task.id)
		}
	}
	return retErr
}

func (s *Runtime) Continue(tasks []int64) {
	s.tasksLock.RLock()
	defer s.tasksLock.RUnlock()
	for _, id := range tasks {
		if task, ok := s.tasks[model.ID(id)]; ok {
			task.Continue()
		}
		// TODO: Report missed tasks.
	}
}

func (s *Runtime) Pause(tasks []int64) error {
	s.tasksLock.RLock()
	defer s.tasksLock.RUnlock()
	for _, id := range tasks {
		if task, ok := s.tasks[model.ID(id)]; ok {
			err := task.Pause()
			if err != nil {
				return err
			}
		} else {
			return errors.ErrTaskNotFound.FastGenByArgs(id)
		}
	}
	return nil
}

func (s *Runtime) Query(taskID int64) *pb.TaskStatus {
	s.tasksLock.RLock()
	defer s.tasksLock.RUnlock()
	status := &pb.TaskStatus{
		Id: taskID,
	}
	if task, ok := s.tasks[model.ID(taskID)]; ok {
		ts := task.GetStatus()
		switch ts {
		case Runnable, Waking:
			status.Status = pb.TaskStatusType_Running
		case Blocked:
			status.Status = pb.TaskStatusType_Blocked
		case Stop:
			status.Status = pb.TaskStatusType_Finished
		case Paused:
			status.Status = pb.TaskStatusType_Pause
		default:
			err := errors.ErrUnknownExecutorID.FastGenByArgs(taskID, ts)
			status.Err = &pb.Error{
				Message: err.Error(),
			}
		}
	} else {
		status.Status = pb.TaskStatusType_NotFound
	}
	return status
}

func (s *Runtime) Run(ctx context.Context, cur int) {
	s.wg.Add(cur)
	for i := 0; i < cur; i++ {
		go s.runImpl(ctx)
	}
	s.wg.Wait()
}

func (s *Runtime) runImpl(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t := s.q.pop()
		if t == nil {
			// idle, sleep for sometime to avoid busy loop
			// TODO: find better wake up mechanism way if needed
			time.Sleep(time.Millisecond * 50)
			continue
		}
		status := t.Poll()
		if status == Blocked {
			if t.tryBlock() {
				continue
			}
			// the status is waking
		} else if status == Stop {
			continue
		}
		t.setRunnable()
		s.q.push(t)
	}
}

func NewRuntime(ctx *test.Context) *Runtime {
	s := &Runtime{
		testCtx: ctx,
	}
	s.hb = NewHeartbeatServer(s)
	return s
}
