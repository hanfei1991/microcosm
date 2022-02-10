package servermaster

import (
	"sync"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type jobHolder struct {
	lib.WorkerHandle
	*model.JobMasterV2
}

type JobFsm struct {
	jobsMu      sync.Mutex
	pendingJobs map[string]*model.JobMasterV2
	waitAckJobs map[string]*model.JobMasterV2
	onlineJobs  map[lib.WorkerID]*jobHolder
}

func NewJobFsm() *JobFsm {
	return &JobFsm{
		pendingJobs: make(map[string]*model.JobMasterV2),
		waitAckJobs: make(map[string]*model.JobMasterV2),
		onlineJobs:  make(map[lib.WorkerID]*jobHolder),
	}
}

func (fsm *JobFsm) JobDispatched(job *model.JobMasterV2) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()
	fsm.waitAckJobs[job.ID] = job
}

func (fsm *JobFsm) IterPendingJobs(dispatchJobFn func(job *model.JobMasterV2) (string, error)) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	for oldJobID, job := range fsm.pendingJobs {
		id, err := dispatchJobFn(job)
		if err != nil {
			return err
		}
		delete(fsm.pendingJobs, oldJobID)
		job.ID = id
		fsm.waitAckJobs[id] = job
		log.L().Info("job master recovered", zap.Any("job", job))
	}

	return nil
}

func (fsm *JobFsm) JobOnline(worker lib.WorkerHandle) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	cfg, ok := fsm.waitAckJobs[worker.ID()]
	if !ok {
		return errors.ErrWorkerNotFound.GenWithStackByArgs(worker.ID())
	}
	fsm.onlineJobs[worker.ID()] = &jobHolder{
		WorkerHandle: worker,
		JobMasterV2:  cfg,
	}
	return nil

}

func (fsm *JobFsm) JobOffline(worker lib.WorkerHandle) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	job, ok := fsm.onlineJobs[worker.ID()]
	if !ok {
		log.L().Warn("non-online worker offline, ignore it", zap.String("id", worker.ID()))
		return
	}
	fsm.pendingJobs[worker.ID()] = job.JobMasterV2
}
