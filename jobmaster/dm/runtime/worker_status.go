package runtime

import (
	"github.com/hanfei1991/microcosm/lib"
)

type WorkerStage int

const (
	WorkerCreating WorkerStage = iota
	WorkerOnline
	WorkerFinished
	WorkerOffline
	// WorkerDestroying
)

type WorkerStatus struct {
	Task  string
	ID    lib.WorkerID
	Unit  lib.WorkerType
	Stage WorkerStage
}

func (w *WorkerStatus) IsOffline() bool {
	return w.Stage == WorkerOffline
}

func (w *WorkerStatus) IsExpected() bool {
	return w.Stage == WorkerOnline || w.Stage == WorkerCreating || w.Stage == WorkerFinished
}

func NewWorkerStatus(task string, unit lib.WorkerType, id lib.WorkerID, stage WorkerStage) WorkerStatus {
	return WorkerStatus{
		Task:  task,
		ID:    id,
		Unit:  unit,
		Stage: stage,
	}
}
