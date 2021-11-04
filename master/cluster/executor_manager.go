package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/autoid"
	"github.com/hanfei1991/microcosom/pkg/ha"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
)

var _ ExecutorClient = &ExecutorManager{}
var _ ResourceMgr = &ExecutorManager{}

// ExecutorManager holds all the executors info, including liveness, status, resource usage.
type ExecutorManager struct {

	// TODO: We should abstract a ha store to handle.
	etcdCli *clientv3.Client

	mu sync.Mutex
	executors map[model.ExecutorID]*Executor
//	migrateTasks []*model.Task

	idAllocator *autoid.Allocator
	haStore ha.HAStore
}

func (e *ExecutorManager) removeExecutor(id model.ExecutorID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	exec, ok := e.executors[id]
	if !ok {
		return errors.New("cannot find executor")
	}
	//for _, t := range exec.resource.Tasks {
	//	e.migrateTasks = append(e.migrateTasks, t.Task)
	//}
	delete(e.executors, id)
	err := e.haStore.Del(exec.EtcdKey())
	if err != nil {
		return err
	}
	return nil
}

func (e *ExecutorManager) HandleHeartbeat(req * pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.executors[model.ExecutorID(req.ExecutorId)]
	if !ok {
		return &pb.HeartbeatResponse{ErrMessage: "not found executor"}, nil
	}
	exec.lastUpdateTime = time.Unix(int64(req.Timestamp), 0)
	exec.heartbeatTTL = time.Duration(req.Ttl) * time.Millisecond
	usage := ResourceUsage(req.ResourceUsage)
	exec.resource.Used = usage
	resp := &pb.HeartbeatResponse{}
	for _, taskRes := range req.Tasks {
		id := taskRes.TaskId
		used := taskRes.ResourceUsage
		if t, ok := exec.resource.Tasks[model.TaskID(id)]; !ok {
			resp.ErrMessage = "can find task"	
		} else {
			t.Used = ResourceUsage(used)
		}
	}
	return resp, nil
}

// AddExecutor processes the `RegisterExecutorRequest`.
// 
func (e *ExecutorManager) AddExecutor(req *pb.RegisterExecutorRequest) (*model.ExecutorInfo, error) {

	e.mu.Lock()
	info := &model.ExecutorInfo{
		ID: model.ExecutorID(s.idAllocator.AllocID()),
		Addr: req.Address,
		Capability: int(req.Capability),
	}
	if _, ok := e.executors[info.ID]; ok {
		e.mu.Unlock()
		return nil, errors.Errorf("Executor has been registered")
	}
	e.mu.Unlock()	

	// Following part is to bootstrap the executor.
	exec := &Executor{ExecutorInfo: *info}
	var err error
	exec.client, err = newExecutorClient(info.Addr)
	if err != nil {
		return nil, err
	}
	// Persistant
	value, err := info.ToJSON()
	if err != nil {
		return nil, err
	}
	e.haStore.Put(info.EtcdKey(), value)	

	e.mu.Lock()
	e.executors[info.ID] = exec
	e.mu.Unlock()
	return info, nil
}

type ExecutorStatus int32

const (
	Running ExecutorStatus = iota
	Disconnected
	Tombstone
)

type Executor struct {
	model.ExecutorInfo
	Status ExecutorStatus
	resource ExecutorResource

	// Executor is busy and will not accept any new task.
	Busy bool

	mu sync.Mutex
	// Last heartbeat
	lastUpdateTime time.Time
	heartbeatTTL   time.Duration

	client *executorClient
}

func (e *Executor) checkAlive() bool {
	if atomic.LoadInt32((*int32)(&e.Status)) == int32(Tombstone) {
		return false	
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lastUpdateTime.Add(e.heartbeatTTL).Before(time.Now()) {
		atomic.StoreInt32((*int32)(&e.Status), int32(Tombstone))
		return false	
	}
	return true
}

func (e *ExecutorManager) check() error {
	e.mu.Lock()
	for id, exec := range e.executors {
		if !exec.checkAlive() {
			e.mu.Unlock()
			err := e.removeExecutor(id)
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
	e.mu.Lock()
	resp, err := exec.client.send(ctx, req)
	if err != nil {
		atomic.CompareAndSwapInt32((*int32)(&exec.Status), int32(Running), int32(Disconnected))
		return resp, err
	}
	return resp, nil
}

// Resource is the min unit of resource that we count.
type ResourceUsage int

//type Task struct {
//	*model.Task
//	Reserved ResourceUnit
//	Used     ResourceUnit
//}

type ExecutorResource struct {
	ID model.ExecutorID

	Capacity ResourceUsage
	Reserved ResourceUsage
	Used     ResourceUsage
	//Tasks    map[model.TaskID]*Task
}

//func (e *ExecutorResource) appendTask(t *model.Task) {
//	task := &Task{
//		Task: t,
//		Reserved: ResourceUnit(t.Cost),
//	}
//	e.Tasks[t.ID] = task
//	e.Reserved += task.Reserved
//}

func (e *ExecutorResource) getSnapShot() *ExecutorResource {
	r := &ExecutorResource{
		Capacity: e.Capacity,
		Reserved: e.Reserved,
		Used: e.Used,
	}
	//for _, t:= range e.Tasks {
	//	r.Tasks[t.ID] = t
	//}
	return r
}
