package master

import (
	"github.com/hanfei1991/microcosom/model"
)

type JobMaster interface {
	DispatchJob() error
	OfflineExecutor(eid model.ExecutorID)
	ID() model.JobID
}
