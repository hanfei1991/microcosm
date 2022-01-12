package client

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
)

const (
	heartbeatTimeout  time.Duration = 8 * time.Second
	heartbeatInterval time.Duration = 1 * time.Second
)

type executorProber struct {
	sync.Mutex
	ctx                context.Context
	probeList          sync.Map
	newProbeChannel    chan []model.ID
	removeProbeChannel chan []model.ID
	statsUpdateChannel chan []*pb.TaskStatus
	pbClient           pb.ExecutorClient
	lastRespondTime    time.Time
	closed             bool
	onClose            func()
	executorID         model.ExecutorID
	//	address    string
	workerPool workerpool.AsyncPool
}

func (e *executorProber) processTaskStatus() {
	select {
	case <-e.ctx.Done():
		return
	case stats := <-e.statsUpdateChannel:
		for _, stat := range stats {
			id := model.ID(stat.Id)
			value, ok := e.probeList.Load(id)
			if !ok {

			}
			err := e.workerPool.Go(e.ctx, func() {
				value.(func(*pb.TaskStatus))(stat)
			})
			if err != nil {
				// that means the worker pool has been closed.
				return
			}
		}
	}
}

func (e *executorProber) close() {
	e.Lock()
	e.closed = true
	e.onClose() // remove self from outside
	e.Unlock()
	// Then do clean job.
	e.probeList.Range(func(key, value interface{}) bool {
		id := key.(model.ID)
		cb := value.(func(*pb.TaskStatus))
		stats := &pb.TaskStatus{
			Id:     int64(id),
			Status: pb.TaskStatusType_Err,
			Err: &pb.Error{
				Code: pb.ErrorCode_NodeFailed,
			},
		}
		cb(stats)
		return true
	})
}

func (e *executorProber) register(tidList []model.ID, cbList []func(*pb.TaskStatus)) error {
	e.Lock()
	defer e.Unlock()
	// executor is closing.
	if e.closed {
		return errors.New("executor has been closed")
	}
	for i := range tidList {
		e.probeList.Store(tidList[i], cbList[i])
	}
	e.newProbeChannel <- tidList
	return nil
}
func (e *executorProber) unRegister(tidList []model.ID) error {
	e.Lock()
	defer e.Unlock()
	// executor is closing.
	if e.closed {
		return errors.New("executor has been closed")
	}
	for i := range tidList {
		e.probeList.Delete(tidList[i])
	}
	e.newProbeChannel <- tidList
	return nil
}

func (e *executorProber) run() {
	ticker := time.NewTicker(heartbeatInterval)
	newTasks := make(map[model.ID]struct{})
	unRegisterTasks := make(map[model.ID]struct{})
	for {
		if e.lastRespondTime.Add(heartbeatTimeout).Before(time.Now()) {
			e.close()
			return
		}
		select {
		case <-e.ctx.Done():
			e.close()
			return
		case newTaskList := <-e.newProbeChannel:
			for _, id := range newTaskList {
				newTasks[id] = struct{}{}
				delete(unRegisterTasks, id)
			}
		case removeTaskList := <-e.removeProbeChannel:
			for _, id := range removeTaskList {
				unRegisterTasks[id] = struct{}{}
				delete(newTasks, id)
			}
		case <-ticker.C:
		}
		req := &pb.ExecutorHeartbeatRequest{}
		for tid := range newTasks {
			req.RegisterTaskIdList = append(req.RegisterTaskIdList, int64(tid))
		}
		for tid := range unRegisterTasks {
			req.UnregisterTaskIdList = append(req.UnregisterTaskIdList, int64(tid))
		}
		resp, err := e.pbClient.Heartbeat(e.ctx, req)
		if err != nil {
			log.L().Logger.Info("executor heartbeat meets error")
			continue
		}
		e.statsUpdateChannel <- resp.TaskStatus
		e.lastRespondTime = time.Now()
		newTasks = make(map[model.ID]struct{})
		unRegisterTasks = make(map[model.ID]struct{})
	}
}

func (e *executorClient) Register(tidList []model.ID, cbList []func(*pb.TaskStatus)) error {
	return e.prober.register(tidList, cbList)
}

func (e *executorClient) UnRegister(tidList []model.ID) error {
	return e.prober.unRegister(tidList)
}
