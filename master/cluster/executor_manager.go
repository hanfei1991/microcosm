package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/etcdutil"
	"github.com/hanfei1991/microcosom/pkg/log"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

var _ ExecutorClient = &ExecutorManager{}
var _ ResourceMgr = &ExecutorManager{}

// ExecutorManager holds all the executors info, including liveness, status, resource usage.
type ExecutorManager struct {

	// TODO: We should abstract a ha store to handle.
	etcdCli *clientv3.Client

	mu sync.Mutex
	executors map[model.ExecutorID]*Executor
	migrateTasks []*model.Task
}

func (s *ExecutorManager) RemoveExecutor() {

}

func (s *ExecutorManager) HandleHeartbeat( req * pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.executors[model.ExecutorID(req.ExecutorId)]
	if !ok {
		return &pb.HeartbeatResponse{ErrMessage: "not found executor"}, nil
	}
	exec.lastUpdateTime = time.Unix(int64(req.Timestamp), 0)
	exec.heartbeatTTL = time.Duration(req.Ttl) * time.Millisecond
	usage := ResourceUnit(req.ResourceUsage)
	exec.resource.Used = usage
	resp := &pb.HeartbeatResponse{}
	for _, taskRes := range req.Tasks {
		id := taskRes.TaskId
		used := taskRes.ResourceUsage
		if t, ok := exec.resource.Tasks[model.TaskID(id)]; !ok {
			resp.ErrMessage = "can find task"	
		} else {
			t.Used = ResourceUnit(used)
		}
	}
	return resp, nil
}

func (s *ExecutorManager) AddExecutor(req *pb.RegisterExecutorRequest) (*model.ExecutorInfo, error) {
	info := model.ExecutorInfo{
		Addr: req.Address,
		Capability: int(req.Capability),
	}

	s.mu.Lock()
	if e, ok := s.executors[info.ID]; ok {
		s.mu.Unlock()
		if e.Addr != req.Address {
			return errors.Errorf("Executor %s has been registered", info.Name)
		}
		// TODO: If executor is pulled up again after crashed, we should recover it 
		// by dispatching old tasks again.
		// info.Status = model.ExecutorRecovering
		// err = recover(info)
		// if err != nil {return err}
	}
	s.mu.Unlock()	

	// Following part is to bootstrap the executor.
	info.Status = int(model.ExecutorBootstrap)

	value, err := info.ToJSON()
	if err != nil {
		return err
	}

	e := &Executor{ExecutorInfo: info}
	e.client, err = newExecutorClient(info.Addr)
	if err != nil {
		return err
	}
	// Persistant
	_,_, err = etcdutil.DoOpsInOneTxnWithRetry(s.etcdCli, clientv3.OpPut(ExecutorKeyAdapter.Encode(info.Name), value))
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.executors[info.Name] = e
	s.mu.Unlock()
	return nil
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

	// Last heartbeat
	lastUpdateTime time.Time
	heartbeatTTL   time.Duration

	client *executorClient
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
type ResourceUnit int

type Task struct {
	*model.Task
	Reserved ResourceUnit
	Used     ResourceUnit
}

type ExecutorResource struct {
	Capacity ResourceUnit
	Reserved ResourceUnit
	Used     ResourceUnit
	Tasks    map[model.TaskID]*Task
}

func (e *ExecutorResource) appendTask(t *model.Task) {
	task := &Task{
		Task: t,
		Reserved: ResourceUnit(t.Cost),
	}
	e.Tasks[t.ID] = task
	e.Reserved += task.Reserved
}

func (e *ExecutorResource) getSnapShot() *ExecutorResource {
	r := &ExecutorResource{
		Capacity: e.Capacity,
		Reserved: e.Reserved,
		Used: e.Used,
	}
	for _, t:= range e.Tasks {
		r.Tasks[t.ID] = t
	}
	return r
}
