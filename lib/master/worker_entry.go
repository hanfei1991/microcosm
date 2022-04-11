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
	id         libModel.WorkerID
	executorID model.ExecutorID

	mu          sync.Mutex
	expireAt    time.Time
	isTombstone bool

	statusReaderMu sync.RWMutex
	statusReader   *statusutil.Reader

	heartbeatCount atomic.Int64
	isOffline      atomic.Bool
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
		id:           id,
		executorID:   executorID,
		expireAt:     expireAt,
		isTombstone:  isTombstone,
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

	e.isTombstone = true
}

func (e *workerEntry) IsTombstone() bool {
	return e.isTombstone
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

func (e *workerEntry) SetExpireTime(expireAt time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.expireAt = expireAt
}

func (e *workerEntry) ExpireTime() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.expireAt
}
