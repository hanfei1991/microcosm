package servermaster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib/master"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pb"
)

func TestJobFsmStateTrans(t *testing.T) {
	t.Parallel()

	fsm := NewJobFsm()

	id := "fsm-test-job-master-1"
	job := &libModel.MasterMetaKVData{
		ID:     id,
		Config: []byte("simple config"),
	}

	// create new job, enter into WaitAckack job queue
	fsm.JobDispatched(job, false)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// OnWorkerOnline, WaitAck -> Online
	err := fsm.JobOnline(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &libModel.WorkerStatus{Code: libModel.WorkerStatusNormal},
		ExecutorID:   "executor-1",
	})
	require.Nil(t, err)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_online))

	// OnWorkerOffline, Online -> Pending
	fsm.JobOffline(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &libModel.WorkerStatus{Code: libModel.WorkerStatusNormal},
		IsTombstone:  true,
	}, true /* needFailover */)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_online))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_pending))

	// Tick, process pending jobs, Pending -> WaitAck
	dispatchedJobs := make([]*libModel.MasterMetaKVData, 0)
	err = fsm.IterPendingJobs(func(job *libModel.MasterMetaKVData) (string, error) {
		dispatchedJobs = append(dispatchedJobs, job)
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_pending))
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// Dispatch job meets error, WaitAck -> Pending
	err = fsm.JobDispatchFailed(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &libModel.WorkerStatus{Code: libModel.WorkerStatusNormal},
		IsTombstone:  true,
	})
	require.Nil(t, err)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_pending))
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// Tick, Pending -> WaitAck
	err = fsm.IterPendingJobs(func(job *libModel.MasterMetaKVData) (string, error) {
		return id, nil
	})
	require.Nil(t, err)
	require.Equal(t, 1, fsm.JobCount(pb.QueryJobResponse_dispatched))
	// job finished
	fsm.JobOffline(&master.MockHandle{
		WorkerID:     id,
		WorkerStatus: &libModel.WorkerStatus{Code: libModel.WorkerStatusNormal},
		IsTombstone:  true,
	}, false /*needFailover*/)
	require.Equal(t, 0, fsm.JobCount(pb.QueryJobResponse_dispatched))

	// offline invalid job, will do nothing
	invalidWorker := &master.MockHandle{
		WorkerID:     id + "invalid",
		WorkerStatus: &libModel.WorkerStatus{Code: libModel.WorkerStatusNormal},
		ExecutorID:   "executor-1",
	}

	fsm.JobOffline(invalidWorker, true)
}
