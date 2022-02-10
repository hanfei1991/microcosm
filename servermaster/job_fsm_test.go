package servermaster

import (
	"testing"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/stretchr/testify/require"
)

func TestJobFsmStateTrans(t *testing.T) {
	t.Parallel()

	fsm := NewJobFsm()

	id := "fsm-test-job-master-1"
	job := &model.JobMasterV2{
		ID:     id,
		Config: []byte("test-config"),
	}
	worker := lib.NewTombstoneWorkerHandle(id, lib.WorkerStatus{Code: lib.WorkerStatusNormal})
	fsm.JobDispatched(job)
	err := fsm.JobOnline(worker)
	require.Nil(t, err)
	fsm.JobOffline(worker)
	dispatchedJobs := make([]*model.JobMasterV2, 0)
	newID := "fsm-test-job-master-2"
	err = fsm.IterPendingJobs(func(job *model.JobMasterV2) (string, error) {
		dispatchedJobs = append(dispatchedJobs, job)
		return newID, nil
	})
	require.Nil(t, err)
	require.Len(t, dispatchedJobs, 1)
	require.Contains(t, fsm.waitAckJobs, newID)
}
