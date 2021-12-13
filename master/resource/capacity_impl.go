package resource

import (
	"sync"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
)

// CapRescMgr implements ResourceMgr interface, and it uses node capacitiy as
// alloction algorithm
type CapRescMgr struct {
	mu        sync.Mutex
	executors map[model.ExecutorID]*ExecutorResource
}

func NewCapRescMgr() *CapRescMgr {
	return &CapRescMgr{
		executors: make(map[model.ExecutorID]*ExecutorResource),
	}
}

// Register implements RescMgr.Register
func (m *CapRescMgr) Register(id model.ExecutorID, capacity RescUnit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executors[id] = &ExecutorResource{
		ID:       id,
		Capacity: capacity,
	}
}

// Allocate implements RescMgr.Allocate
func (m *CapRescMgr) Allocate(tasks []*pb.ScheduleTask) (bool, *pb.TaskSchedulerResponse) {
	return m.allocateTasksWithNaiveStrategy(tasks)
}

// Update implements RescMgr.Update
func (m *CapRescMgr) Update(id model.ExecutorID, use RescUnit, status model.ExecutorStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exec, ok := m.executors[id]
	if !ok {
		return errors.ErrUnknownExecutorID.GenWithStackByArgs(id)
	}
	exec.Used = use
	exec.Status = status
	return nil
}

// getAvailableResource returns resources that are available
func (m *CapRescMgr) getAvailableResource() []*ExecutorResource {
	res := make([]*ExecutorResource, 0)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, exec := range m.executors {
		if exec.Status == model.Running &&
			exec.Capacity > exec.Reserved && exec.Capacity > exec.Used {
			res = append(res, exec)
		}
	}
	return res
}

func (m *CapRescMgr) allocateTasksWithNaiveStrategy(
	tasks []*pb.ScheduleTask,
) (bool, *pb.TaskSchedulerResponse) {
	result := make(map[int32]*pb.ScheduleResult)
	resources := m.getAvailableResource()
	var idx int = 0
	for _, task := range tasks {
		originalIdx := idx
		for {
			exec := resources[idx]
			used := exec.Used
			if exec.Reserved > used {
				used = exec.Reserved
			}
			rest := exec.Capacity - used
			if rest >= RescUnit(task.Cost) {
				result[task.GetTask().Id] = &pb.ScheduleResult{
					ExecutorId: int32(exec.ID),
				}
				exec.Reserved = exec.Reserved + RescUnit(task.GetCost())
				break
			}
			idx = (idx + 1) % len(resources)
			if idx == originalIdx {
				return false, nil
			}
		}
	}
	return true, &pb.TaskSchedulerResponse{Schedule: result}
}
