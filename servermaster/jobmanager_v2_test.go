package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestJobManagerSubmitJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl(lib.MasterID("submit-job-test"))
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.TaskSchedulerResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster:  mockMaster.BaseMaster,
		idAllocator: autoid.NewJobIDAllocator(),
		jobMasters:  make(map[model.ID]*model.Task),
	}
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_Benchmark,
		Config: []byte(""),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)
	time.Sleep(time.Millisecond * 10)
	// TODO: use job manager v2 as master impl of mock master and check
	// OnWorkerDispatched is called
}
