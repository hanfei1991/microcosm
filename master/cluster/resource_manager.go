package cluster

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/hanfei1991/microcosom/model"
)

// ResouceManager manages the resources of the clusters.
// It records the resource usage of every executors and the running tasks.
type ResourceMgr interface {
	GetResourceSnapshot() (*ResourceSnapshot)
	ApplyNewTasks([]*model.Task)

	GetRescheduleTask() *model.Task
}

type ResourceSnapshot struct {
	executors []*ExecutorResource
}

func (r *ExecutorManager) ApplyMigrateTasks(oldID model.ExecutorID, newTask *model.Task) {
	e, ok := r.executors[oldID]
	if ok {
		_, ok = e.resource.Tasks[newTask.ID]
		if !ok {
			// Not expected
		}
		delete(e.resource.Tasks, newTask.ID)
		e.resource.Reserved -= ResourceUnit(newTask.Cost)
	}
	e, ok = r.executors[newTask.ExecutorID]
	if !ok {
		// new executor has been dead, need to reschedule again
		r.migrateTasks = append(r.migrateTasks, newTask)
	} else {
		e.resource.appendTask(newTask)
	}

}

func (r *ExecutorManager) ApplyNewTasks(tasks []*model.Task) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range tasks {
		e := t.ExecutorID
		if exec, ok := r.executors[e]; !ok || exec.Status == Tombstone {
			r.migrateTasks = append(r.migrateTasks, t)
		} else {
			exec.resource.appendTask(t)
		}
	}
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

func (r *ExecutorManager) GetRescheduleTask() *model.Task {
	r.mu.Lock()
	r.mu.Unlock()
	if len(r.migrateTasks) != 0 {
		t := r.migrateTasks[0]
		r.migrateTasks = r.migrateTasks[1:]
		return t
	}

	// We choose the hottest task in the hottest executor.
	var hotExec model.ExecutorID = -1
	var maxUsage ResourceUnit

	for id, e := range r.executors {
		if e.resource.Used * 2 > e.resource.Capacity && e.resource.Used > maxUsage {
			hotExec = id
			maxUsage = e.resource.Used
		}
	}
	if hotExec == -1 {
		return nil
	}
	e := r.executors[hotExec]
	maxUsage = ResourceUnit(0)
	var hotTask model.TaskID = -1
	for id, t := range e.resource.Tasks {
		if t.Used > maxUsage {
			hotTask = id
			maxUsage = t.Used
		}
	}
	return e.resource.Tasks[hotTask].Task
}