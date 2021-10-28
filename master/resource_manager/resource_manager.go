package resource 

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
	ApplyNewTasks(map[model.ExecutorID][]*model.Task)
	// 
	UpdateExecutorStats() 

	RegisterNewExecutor()
	UnRegisterExecutor()
	GetRescheduleTxn() *RescheduleTxn
}

type ResourceSnapshot struct {
	sync.Mutex
	executors []*ExecutorStats
}

func (r *ResourceSnapshot) GetSnapshot() *ResourceSnapshot {
}

type ExecutorStats struct {
	id 	  model.ExecutorID

	cap   int
	usage int
	tasks []*TaskStats // cost and reserved
}

type TaskStats struct {
	*model.Task

	Reserved int
	usage    int
}

type resourceMgr struct {
	resource   ResourceSnapshot
	scheduleMu sync.Mutex
	rescheduleNotifyer func(*RescheduleTxn) (model.ExecutorID , error)
}

func (m *resourceMgr) UpdateExecutorStats() {
}


func (m *resourceMgr) GetResourceSnapshot() *ResourceSnapshot {
	return m.resource.GetSnapshot()
}

func (m *resourceMgr) GetRescheduleTxn() *RescheduleTxn {
	// check if need to reschedule
}


type RescheduleTxn struct {
	JID model.JobID
	TID model.TaskID
	Snapshot *ResourceSnapshot
}