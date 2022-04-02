package master

import (
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
)

type workerEntry struct {
	ID         libModel.WorkerID
	ExecutorID model.ExecutorID

	mu          sync.Mutex
	ExpireAt    time.Time
	IsTombstone bool

	statusReaderMu sync.RWMutex
	statusReader   *statusutil.Reader

	heartbeatCount atomic.Int64
}

func newWorkerEntry(
	id libModel.WorkerID,
	executorID model.ExecutorID,
	expireAt time.Time,
	isTombstone bool,
	initWorkerStatus *libModel.WorkerStatus,
) *workerEntry {
	var stReader *statusutil.Reader
	if initWorkerStatus != nil {
		stReader = statusutil.NewReader(initWorkerStatus)
	}
	return &workerEntry{
		ID:           id,
		ExecutorID:   executorID,
		ExpireAt:     expireAt,
		IsTombstone:  isTombstone,
		statusReader: stReader,
	}
}

func newTombstoneWorkerEntry(
	id libModel.WorkerID,
	lastStatus *libModel.WorkerStatus,
) *workerEntry {
	return newWorkerEntry(id, "", time.Time{}, true, lastStatus)
}

func (e *workerEntry) MarkAsTombstone() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.IsTombstone = true
}

func (e *workerEntry) StatusReader() *statusutil.Reader {
	e.statusReaderMu.RLock()
	defer e.statusReaderMu.RUnlock()

	return e.statusReader
}

func (e *workerEntry) InitStatus(status *libModel.WorkerStatus) {
	e.statusReaderMu.Lock()
	defer e.statusReaderMu.Unlock()

	if e.statusReader != nil {
		log.L().Panic("double InitStatus", zap.Any("worker-entry", e))
	}

	e.statusReader = statusutil.NewReader(status)
}
