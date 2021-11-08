package cluster

import "github.com/hanfei1991/microcosom/model"

// ResouceManager manages the resources of the clusters.
type ResourceMgr interface {
	GetResourceSnapshot() (*ResourceSnapshot)
}

// ResourceSnapshot shows the resource usage of every executors.
type ResourceSnapshot struct {
	Executors []*ExecutorResource
}

// GetResourceSnapshot provides the snapshot of current resource usage.
func (r *ExecutorManager) GetResourceSnapshot() *ResourceSnapshot {
	snapshot := &ResourceSnapshot{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, exec := range r.executors {
		if exec.Status == model.Running && exec.resource.Capacity > exec.resource.Reserved && exec.resource.Capacity > exec.resource.Used{
			snapshot.Executors = append(snapshot.Executors, exec.resource.getSnapShot())
		}
	}
	return snapshot
}
