package scheduler

import (
	"github.com/hanfei1991/microcosm/model"
	schedModel "github.com/hanfei1991/microcosm/servermaster/scheduler/model"
)

// CapacityProvider describes an object providing capacity info for
// each executor.
type CapacityProvider interface {
	// CapacitiesForAllExecutors returns a map of resource statuses for all
	// executors.
	CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus

	// CapacityForExecutor returns the resource status for a given executor.
	// If the executor is not found, (nil, false) is returned.
	CapacityForExecutor(executor model.ExecutorID) (*schedModel.ExecutorResourceStatus, bool)
}

// MockCapacityProvider mocks a CapacityProvider for unit tests.
type MockCapacityProvider struct {
	Capacities map[model.ExecutorID]*schedModel.ExecutorResourceStatus
}

func (p *MockCapacityProvider) CapacitiesForAllExecutors() map[model.ExecutorID]*schedModel.ExecutorResourceStatus {
	return p.Capacities
}

func (p *MockCapacityProvider) CapacityForExecutor(executor model.ExecutorID) (*schedModel.ExecutorResourceStatus, bool) {
	status, ok := p.Capacities[executor]
	return status, ok
}
