package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/master"
	"github.com/hanfei1991/microcosm/lib/metadata"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/errors"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

func TestJobManagerSubmitJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "submit-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.ScheduleTaskResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster:      mockMaster.DefaultBaseMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)
	err = mockMaster.Poll(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return mgr.JobFsm.JobCount(pb.QueryJobResponse_online) == 0 &&
			mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched) == 1 &&
			mgr.JobFsm.JobCount(pb.QueryJobResponse_pending) == 0
	}, time.Second*2, time.Millisecond*20)
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: resp.JobIdStr})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_dispatched, queryResp.Status)
}

type mockBaseMasterCreateWorkerFailed struct {
	*lib.MockMasterImpl
}

func (m *mockBaseMasterCreateWorkerFailed) CreateWorker(
	workerType lib.WorkerType,
	config lib.WorkerConfig,
	cost model.RescUnit,
	resources ...resourcemeta.ResourceID,
) (libModel.WorkerID, error) {
	return "", errors.ErrMasterConcurrencyExceeded.FastGenByArgs()
}

func TestCreateWorkerReturnError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterImpl := lib.NewMockMasterImpl("", "create-worker-with-error")
	mockMaster := &mockBaseMasterCreateWorkerFailed{
		MockMasterImpl: masterImpl,
	}
	mgr := &JobManagerImplV2{
		BaseMaster:      mockMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.NotNil(t, resp.Err)
	require.Regexp(t, ".*ErrMasterConcurrencyExceeded.*", resp.Err.Message)
}

func TestJobManagerPauseJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "pause-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImplV2{
		BaseMaster:      mockMaster.DefaultBaseMaster,
		JobFsm:          NewJobFsm(),
		clocker:         clock.New(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}

	pauseWorkerID := "pause-worker-id"
	meta := &libModel.MasterMetaKVData{ID: pauseWorkerID}
	mgr.JobFsm.JobDispatched(meta, false)

	mockWorkerHandle := &master.MockHandle{WorkerID: pauseWorkerID, ExecutorID: "executor-1"}
	err := mgr.JobFsm.JobOnline(mockWorkerHandle)
	require.Nil(t, err)

	req := &pb.PauseJobRequest{
		JobIdStr: pauseWorkerID,
	}
	resp := mgr.PauseJob(ctx, req)
	require.Nil(t, resp.Err)

	require.Equal(t, 1, mockWorkerHandle.SendMessageCount())

	req.JobIdStr = pauseWorkerID + "-unknown"
	resp = mgr.PauseJob(ctx, req)
	require.NotNil(t, resp.Err)
	require.Equal(t, pb.ErrorCode_UnKnownJob, resp.Err.Code)
}

func TestJobManagerCancelJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "cancel-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		clocker:          clock.New(),
		frameMetaClient:  mockMaster.GetFrameMetaClient(),
		masterMetaClient: metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
	}

	err := mgr.frameMetaClient.UpsertJob(ctx, &libModel.MasterMetaKVData{
		ID:         "job-to-be-canceled",
		Tp:         lib.FakeJobMaster,
		StatusCode: libModel.MasterStatusStopped,
	})
	require.NoError(t, err)

	err = mgr.OnMasterRecovered(ctx)
	require.NoError(t, err)

	resp := mgr.CancelJob(ctx, &pb.CancelJobRequest{
		JobIdStr: "job-to-be-canceled",
	})
	require.Equal(t, &pb.CancelJobResponse{}, resp)
}

func TestJobManagerQueryJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		meta             *libModel.MasterMetaKVData
		expectedPBStatus pb.QueryJobResponse_JobStatus
	}{
		{
			&libModel.MasterMetaKVData{
				ID:         "master-1",
				Tp:         lib.FakeJobMaster,
				StatusCode: libModel.MasterStatusFinished,
			},
			pb.QueryJobResponse_finished,
		},
		{
			&libModel.MasterMetaKVData{
				ID:         "master-2",
				Tp:         lib.FakeJobMaster,
				StatusCode: libModel.MasterStatusStopped,
			},
			pb.QueryJobResponse_stopped,
		},
	}

	mockMaster := lib.NewMockMasterImpl("", "job-manager-query-job-test")
	for _, tc := range testCases {
		cli := metadata.NewMasterMetadataClient(tc.meta.ID, mockMaster.GetFrameMetaClient())
		err := cli.Store(ctx, tc.meta)
		require.Nil(t, err)
	}

	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		frameMetaClient:  mockMaster.GetFrameMetaClient(),
	}

	statuses, err := mgr.GetJobStatuses(ctx)
	require.NoError(t, err)
	require.Len(t, statuses, len(testCases))

	for _, tc := range testCases {
		req := &pb.QueryJobRequest{
			JobId: tc.meta.ID,
		}
		resp := mgr.QueryJob(ctx, req)
		require.Nil(t, resp.Err)
		require.Equal(t, tc.expectedPBStatus, resp.GetStatus())

		require.Contains(t, statuses, tc.meta.ID)
		require.Equal(t, tc.meta.StatusCode, statuses[tc.meta.ID])
	}
}

func TestJobManagerOnlineJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "submit-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.ScheduleTaskResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster:      mockMaster.DefaultBaseMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)

	err = mgr.JobFsm.JobOnline(&master.MockHandle{
		WorkerID:   resp.JobIdStr,
		ExecutorID: "executor-1",
	})
	require.Nil(t, err)
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: resp.JobIdStr})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_online, queryResp.Status)
	require.Equal(t, queryResp.JobMasterInfo.Id, resp.JobIdStr)
}

func TestJobManagerRecover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "job-manager-recover-test")
	// prepare mockvk with two job masters
	meta := []*libModel.MasterMetaKVData{
		{
			ID: "master-1",
			Tp: lib.FakeJobMaster,
		},
		{
			ID: "master-2",
			Tp: lib.FakeJobMaster,
		},
	}
	for _, data := range meta {
		cli := metadata.NewMasterMetadataClient(data.ID, mockMaster.GetFrameMetaClient())
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}

	mgr := &JobManagerImplV2{
		BaseMaster:       mockMaster.DefaultBaseMaster,
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: metadata.NewMasterMetadataClient(metadata.JobManagerUUID, mockMaster.GetFrameMetaClient()),
		frameMetaClient:  mockMaster.GetFrameMetaClient(),
	}
	err := mgr.OnMasterRecovered(ctx)
	require.Nil(t, err)
	require.Equal(t, 2, mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched))
	queryResp := mgr.QueryJob(ctx, &pb.QueryJobRequest{JobId: "master-1"})
	require.Nil(t, queryResp.Err)
	require.Equal(t, pb.QueryJobResponse_dispatched, queryResp.Status)
}

func TestJobManagerTickExceedQuota(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	masterImpl := lib.NewMockMasterImpl("", "create-worker-with-error")
	mockMaster := &mockBaseMasterCreateWorkerFailed{
		MockMasterImpl: masterImpl,
	}
	mgr := &JobManagerImplV2{
		BaseMaster:      mockMaster,
		JobFsm:          NewJobFsm(),
		uuidGen:         uuid.NewGenerator(),
		frameMetaClient: mockMaster.GetFrameMetaClient(),
	}
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.NoError(t, err)

	mgr.JobFsm.JobDispatched(&libModel.MasterMetaKVData{ID: "failover-job-master"}, true)
	// try to recreate failover job master, will meet quota error
	err = mgr.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched))

	// try to recreate failover job master again, will meet quota error again
	err = mgr.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, mgr.JobFsm.JobCount(pb.QueryJobResponse_dispatched))
}
