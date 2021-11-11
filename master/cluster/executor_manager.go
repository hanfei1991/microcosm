package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/autoid"
	"github.com/hanfei1991/microcosom/pkg/ha"
	"github.com/hanfei1991/microcosom/pkg/log"
	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

var _ ExecutorClient = &ExecutorManager{}
var _ ResourceMgr = &ExecutorManager{}

// ExecutorManager holds all the executors info, including liveness, status, resource usage.
type ExecutorManager struct {
	mu          sync.Mutex
	executors   map[model.ExecutorID]*Executor
	offExecutor chan model.ExecutorID

	idAllocator *autoid.Allocator

	// TODO: complete ha store.
	haStore ha.HAStore
}

func NewExecutorManager(offExec chan model.ExecutorID) *ExecutorManager {
	return &ExecutorManager{
		executors:   make(map[model.ExecutorID]*Executor),
		idAllocator: autoid.NewAllocator(),
		offExecutor: offExec,
	}
}

func (e *ExecutorManager) RemoveExecutor(id model.ExecutorID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	log.L().Logger.Info("begin to remove executor", zap.Int32("id", int32(id)))
	exec, ok := e.executors[id]
	if !ok {
		return errors.New("cannot find executor")
	}
	delete(e.executors, id)
	//err := e.haStore.Del(exec.EtcdKey())
	//if err != nil {
	//	return err
	//}
	e.offExecutor <- id
	log.L().Logger.Info("notify to offline exec")
	return exec.close()
}

func (e *ExecutorManager) HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	log.L().Logger.Info("handle heart beat", zap.Int32("id", req.ExecutorId))
	defer log.L().Logger.Info("end handle heart beat", zap.Int32("id", req.ExecutorId))
	e.mu.Lock()
	exec, ok := e.executors[model.ExecutorID(req.ExecutorId)]
	if !ok {
		e.mu.Unlock()
		return &pb.HeartbeatResponse{ErrMessage: fmt.Sprintf("can't not find executor %d", req.ExecutorId)}, nil
	}
	e.mu.Unlock()
	exec.mu.Lock()
	exec.lastUpdateTime = time.Unix(int64(req.Timestamp), 0)
	exec.heartbeatTTL = time.Duration(req.Ttl) * time.Millisecond
	usage := ResourceUsage(req.ResourceUsage)
	exec.resource.Used = usage
	exec.mu.Unlock()
	resp := &pb.HeartbeatResponse{}
	return resp, nil
}

// AddExecutor processes the `RegisterExecutorRequest`.
func (e *ExecutorManager) AddExecutor(req *pb.RegisterExecutorRequest) (*model.ExecutorInfo, error) {
	log.L().Logger.Info("add executor", zap.String("addr", req.Address))
	e.mu.Lock()
	info := &model.ExecutorInfo{
		ID:         model.ExecutorID(e.idAllocator.AllocID()),
		Addr:       req.Address,
		Capability: int(req.Capability),
	}
	if _, ok := e.executors[info.ID]; ok {
		e.mu.Unlock()
		return nil, errors.Errorf("Executor has been registered")
	}
	e.mu.Unlock()

	// Following part is to bootstrap the executor.
	exec := &Executor{
		ExecutorInfo: *info,
		resource: ExecutorResource{
			ID:       info.ID,
			Capacity: ResourceUsage(info.Capability),
		},
		Status: model.Running,
	}
	var err error
	exec.client, err = newExecutorClient(info.Addr)
	if err != nil {
		return nil, err
	}
	// Persistant
	//value, err := info.ToJSON()
	//if err != nil {
	//return nil, err
	//}
	//	e.haStore.Put(info.EtcdKey(), value)

	e.mu.Lock()
	e.executors[info.ID] = exec
	e.mu.Unlock()
	return info, nil
}

type Executor struct {
	model.ExecutorInfo
	Status   model.ExecutorStatus
	resource ExecutorResource

	mu sync.Mutex
	// Last heartbeat
	lastUpdateTime time.Time
	heartbeatTTL   time.Duration

	client *executorClient
}

func (e *Executor) close() error {
	return e.client.close()
}

func (e *Executor) checkAlive() bool {
	log.L().Logger.Info("check alive", zap.Int32("exec", int32(e.ExecutorInfo.ID)))
	defer log.L().Logger.Info("end check alive", zap.Int32("exec", int32(e.ExecutorInfo.ID)))
	if atomic.LoadInt32((*int32)(&e.Status)) == int32(model.Tombstone) {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.lastUpdateTime.IsZero() && e.lastUpdateTime.Add(e.heartbeatTTL).Before(time.Now()) {
		atomic.StoreInt32((*int32)(&e.Status), int32(model.Tombstone))
		return false
	}
	return true
}

func (e *ExecutorManager) Start(ctx context.Context) {
	go e.checkAlive(ctx)
}

func (e *ExecutorManager) checkAlive(ctx context.Context) {
	tick := time.NewTicker(1 * time.Second)
	defer func() { log.L().Logger.Info("check alive finished") }()
	for {
		select {
		case <-tick.C:
			err := e.checkAliveImpl()
			if err != nil {
				log.L().Logger.Info("check alive meet error", zap.Error(err))
			}
		case <-ctx.Done():
		}
	}
}

func (e *ExecutorManager) checkAliveImpl() error {
	e.mu.Lock()
	for id, exec := range e.executors {
		if !exec.checkAlive() {
			e.mu.Unlock()
			err := e.RemoveExecutor(id)
			return err
		}
	}
	e.mu.Unlock()
	return nil
}

func (e *ExecutorManager) Send(ctx context.Context, id model.ExecutorID, req *ExecutorRequest) (*ExecutorResponse, error) {
	e.mu.Lock()
	exec, ok := e.executors[id]
	if !ok {
		e.mu.Unlock()
		return nil, errors.New("No such executor")
	}
	e.mu.Unlock()
	resp, err := exec.client.send(ctx, req)
	if err != nil {
		atomic.CompareAndSwapInt32((*int32)(&exec.Status), int32(model.Running), int32(model.Disconnected))
		return resp, err
	}
	return resp, nil
}
