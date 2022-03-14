package resource

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource"
)

var _ RescMgr = &CapRescMgr{}

// CapRescMgr implements ResourceMgr interface, and it uses node capacity as
// alloction algorithm
type CapRescMgr struct {
	mu        sync.Mutex
	r         *rand.Rand // random generator for choosing node
	executors map[model.ExecutorID]*ExecutorResource
	startIdx  int

	// The thunk is used here because the external resource
	// manager is intialized lazily in Server.
	// TODO resolve this issue by proper dependency management!
	externalResourceManagerThunk func() *externalresource.Manager
}

func NewCapRescMgr(managerThunk func() *externalresource.Manager) *CapRescMgr {
	return &CapRescMgr{
		r:         rand.New(rand.NewSource(time.Now().UnixNano())),
		executors: make(map[model.ExecutorID]*ExecutorResource),
		startIdx:  rand.Int(),
	}
}

// Register implements RescMgr.Register
func (m *CapRescMgr) Register(id model.ExecutorID, addr string, capacity model.RescUnit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executors[id] = &ExecutorResource{
		ID:       id,
		Capacity: capacity,
		Addr:     addr,
	}
	log.L().Info("executor resource is registered",
		zap.String("executor-id", string(id)), zap.Int("capacity", int(capacity)))
}

// Unregister implements RescMgr.Unregister
func (m *CapRescMgr) Unregister(id model.ExecutorID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.executors, id)
	log.L().Info("executor resource is unregistered",
		zap.String("executor-id", string(id)))
}

// Allocate implements RescMgr.Allocate
func (m *CapRescMgr) Allocate(ctx context.Context, task *pb.ScheduleTask) (bool, *pb.ScheduleResult) {
	if len(task.RequiredResources) == 0 {
		return m.allocateTasksWithNaiveStrategy(model.RescUnit(task.Cost))
	}

	externalResourceManager := m.externalResourceManagerThunk()
	if externalResourceManager == nil {
		// TODO return a proper error here
		return false, nil
	}

	var target model.ExecutorID
	for _, resourceID := range task.GetRequiredResources() {
		newTarget, err := externalResourceManager.GetExecutorConstraint(ctx, resourceID)
		if err != nil {
			log.L().Warn("Capacity Manager failed to request the contraint on resource",
				zap.Any("task", task), zap.String("failed-resource-id", resourceID))
			return false, nil
		}
		if target != "" && newTarget != target {
			log.L().Info("Conflicting requirements",
				zap.Any("task", task))
			return false, nil
		}
	}

	return m.allocateTaskWithConstraint(model.RescUnit(task.Cost), target)
}

// Update implements RescMgr.Update
func (m *CapRescMgr) Update(id model.ExecutorID, used, reserved model.RescUnit, status model.ExecutorStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exec, ok := m.executors[id]
	if !ok {
		return errors.ErrUnknownExecutorID.GenWithStackByArgs(id)
	}
	exec.Used = used
	exec.Reserved = reserved
	exec.Status = status
	return nil
}

// getAvailableResource returns resources that are available
func (m *CapRescMgr) getAvailableResource() []*ExecutorResource {
	res := make([]*ExecutorResource, 0)
	for _, exec := range m.executors {
		if exec.Status == model.Running &&
			exec.Capacity > exec.Reserved && exec.Capacity > exec.Used {
			res = append(res, exec)
		}
	}
	return res
}

func (m *CapRescMgr) allocateTasksWithNaiveStrategy(
	taskCost model.RescUnit,
) (successful bool, result *pb.ScheduleResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	resources := m.getAvailableResource()
	if len(resources) == 0 {
		// No resources in this cluster
		return false, nil
	}

	originalIdx := m.r.Intn(len(resources))
	idx := originalIdx
	for {
		exec := resources[idx]
		used := exec.Used
		if exec.Reserved > used {
			used = exec.Reserved
		}
		rest := exec.Capacity - used
		if rest >= taskCost {
			result = &pb.ScheduleResult{
				ExecutorId: string(exec.ID),
				Addr:       exec.Addr,
			}
			break
		}
		idx = (idx + 1) % len(resources)
		m.startIdx++
		if idx == originalIdx {
			return false, nil
		}
	}
	return true, result
}

func (m *CapRescMgr) allocateTaskWithConstraint(
	taskCost model.RescUnit,
	constraint model.ExecutorID,
) (successful bool, result *pb.ScheduleResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	exec, ok := m.executors[constraint]
	if !ok {
		return false, nil
	}

	used := exec.Used
	if exec.Reserved > used {
		used = exec.Reserved
	}
	rest := exec.Capacity - used
	if rest < taskCost {
		return false, nil
	}

	return true, &pb.ScheduleResult{
		ExecutorId: string(exec.ID),
		Addr:       exec.Addr,
	}
}
