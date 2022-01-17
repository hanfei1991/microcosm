package client

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/zap"
)

var SelfExecID model.ExecutorID

const (
	heartbeatTimeout  time.Duration = 8 * time.Second
	heartbeatInterval time.Duration = 1 * time.Second
)

type heartbeatClient struct {
	sync.Mutex
	ctx    context.Context
	cancel func()

	watchList          sync.Map
	newWatchChannel    chan model.ID
	unWatchChannel     chan model.ID
	statsUpdateChannel chan *pb.TaskStatus
	pbClient           pb.ExecutorClient
	lastRespondTime    time.Time
	closed             bool
	onClose            func()
	executorID         model.ExecutorID
	//	address    string
	workerPool workerpool.AsyncPool
}

func (e *heartbeatClient) processTaskStatus() {
	select {
	case <-e.ctx.Done():
		return
	case stat := <-e.statsUpdateChannel:
		id := model.ID(stat.Id)
		value, ok := e.watchList.Load(id)
		if !ok {
			// this tid has been unwatched.
			return
		}
		err := e.workerPool.Go(e.ctx, func() {
			value.(func(*pb.TaskStatus))(stat)
		})
		if err != nil {
			// that means the worker pool has been closed.
			panic("worker pool closed, we can't call cbs")
		}
	}
}

func (e *heartbeatClient) close() {
	e.Lock()
	e.closed = true
	e.onClose() // remove self from outside
	e.Unlock()
	e.cancel()
	// Then do clean job.
	e.watchList.Range(func(key, value interface{}) bool {
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

func newHeartbeatClient(eID model.ExecutorID, onClose func(), pbClient pb.ExecutorClient) *heartbeatClient {
	hbClient := &heartbeatClient{
		newWatchChannel:    make(chan model.ID, 128),
		unWatchChannel:     make(chan model.ID, 128),
		statsUpdateChannel: make(chan *pb.TaskStatus, 128),
		pbClient:           pbClient,
		executorID:         eID, // target id
		onClose:            onClose,
	}
	hbClient.ctx, hbClient.cancel = context.WithCancel(context.Background())
	go hbClient.processTaskStatus()
	go hbClient.run()
	return hbClient
}

func (e *heartbeatClient) register(tid model.ID, cb func(*pb.TaskStatus)) {
	e.Lock()
	defer e.Unlock()
	// executor is closing.
	if e.closed {
		cb(&pb.TaskStatus{Status: pb.TaskStatusType_NotFound})
		return
	}
	e.watchList.Store(tid, cb)
	e.newWatchChannel <- tid
}

func (e *heartbeatClient) unRegister(tid model.ID) {
	e.Lock()
	defer e.Unlock()
	e.watchList.Delete(tid)
	e.unWatchChannel <- tid
}

func (e *heartbeatClient) run() {
	ticker := time.NewTicker(heartbeatInterval)
	newTasks := make(map[model.ID]struct{})
	unRegisterTasks := make(map[model.ID]struct{})
	e.lastRespondTime = time.Now()
	for {
		if e.lastRespondTime.Add(heartbeatTimeout).Before(time.Now()) {
			e.close()
			return
		}
		select {
		case <-e.ctx.Done():
			e.close()
			return
		case id := <-e.newWatchChannel:
			newTasks[id] = struct{}{}
			delete(unRegisterTasks, id)
		case id := <-e.unWatchChannel:
			unRegisterTasks[id] = struct{}{}
			delete(newTasks, id)
		case <-ticker.C:
		}
		req := &pb.ExecutorHeartbeatRequest{}
		for tid := range newTasks {
			req.RegisterTaskIdList = append(req.RegisterTaskIdList, int64(tid))
		}
		for tid := range unRegisterTasks {
			req.UnregisterTaskIdList = append(req.UnregisterTaskIdList, int64(tid))
		}
		req.TargetExecId = string(e.executorID)
		req.SourceExecId = string(SelfExecID)
		resp, err := e.pbClient.Heartbeat(e.ctx, req)
		if err != nil {
			log.L().Logger.Info("executor heartbeat meets error", zap.Error(err))
			continue
		}
		if resp.Err != nil {
			log.L().Logger.Info("executor heartbeat meets error", zap.String("error", resp.Err.Message))
			continue
		}
		e.lastRespondTime = time.Now()
		go func() {
			for _, status := range resp.TaskStatus {
				e.statsUpdateChannel <- status
			}
		}()
		newTasks = make(map[model.ID]struct{})
		unRegisterTasks = make(map[model.ID]struct{})
	}
}

func (e *executorClient) Watch(tid model.ID, cb func(*pb.TaskStatus)) {
	if e.hbClient == nil {
		return
	}
	e.hbClient.register(tid, cb)
}

func (e *executorClient) UnWatch(tid model.ID) {
	if e.hbClient == nil {
		return
	}
	e.hbClient.unRegister(tid)
}
