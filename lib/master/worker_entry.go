package master

import (
	"time"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
)

type workerEntry struct {
	ID         libModel.WorkerID
	ExecutorID model.ExecutorID

	ExpireAt time.Time
}

func newWorkerEntry(id libModel.WorkerID, executorID model.ExecutorID, expireAt time.Time) *workerEntry {
	return &workerEntry{
		ID:         id,
		ExecutorID: executorID,
		ExpireAt:   expireAt,
	}
}
