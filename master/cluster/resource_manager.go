package cluster

import (
)

// ResouceManager manages the resources of the clusters.
type ResourceMgr interface {
	GetResourceSnapshot() (*ResourceSnapshot)
}

type ResourceSnapshot struct {
	executors []*ExecutorResource
}

func (r *ExecutorManager) GetResourceSnapshot() *ResourceSnapshot {
	snapshot := &ResourceSnapshot{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, exec := range r.executors {
		if exec.Status == Running && exec.resource.Capacity > exec.resource.Reserved && exec.resource.Capacity > exec.resource.Used{
			snapshot.executors = append(snapshot.executors, exec.resource.getSnapShot())
		}
	}
	return snapshot
}
