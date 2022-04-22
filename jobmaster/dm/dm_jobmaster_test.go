package dm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	libMetadata "github.com/hanfei1991/microcosm/lib/metadata"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
	kvmock "github.com/hanfei1991/microcosm/pkg/meta/kv/mockclient"
	dorm "github.com/hanfei1991/microcosm/pkg/meta/orm"
	libModel "github.com/hanfei1991/microcosm/pkg/meta/orm/model"
	"github.com/hanfei1991/microcosm/pkg/p2p"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/dig"
)

func TestDMJobmasterSuite(t *testing.T) {
	suite.Run(t, new(testDMJobmasterSuite))
}

type testDMJobmasterSuite struct {
	suite.Suite
}

func (t *testDMJobmasterSuite) SetupSuite() {
	TaskNormalInterval = time.Hour
	TaskErrorInterval = 100 * time.Millisecond
	WorkerNormalInterval = time.Hour
	WorkerErrorInterval = 100 * time.Millisecond
	runtime.HeartbeatInterval = 1 * time.Second
}

type masterParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       *dorm.MetaOpsClient
	UserRawKVClient       kvclient.KVClientEx
	ExecutorClientManager client.ClientsManager
	ServerMasterClient    client.MasterClient
	ResourceBroker        broker.Broker
}

// Init -> Poll -> Close
func (t *testDMJobmasterSuite) TestRunDMJobMaster() {
	mockServerMasterClient := &client.MockServerMasterClient{}
	mockExecutorClient := client.NewClientManager()
	cli, mock, err := dorm.NewMockMetaOpsClient()
	depsForTest := masterParamListForTest{
		MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
		MessageSender:         p2p.NewMockMessageSender(),
		FrameMetaClient:       cli,
		UserRawKVClient:       kvmock.NewMetaMock(),
		ExecutorClientManager: mockExecutorClient,
		ServerMasterClient:    mockServerMasterClient,
		ResourceBroker:        nil,
	}

	RegisterWorker()
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t.T(), dp.Provide(func() masterParamListForTest {
		return depsForTest
	}))
	dctx = dctx.WithDeps(dp)

	// submit-job
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	var b bytes.Buffer
	err := toml.NewEncoder(&b).Encode(jobCfg)
	require.NoError(t.T(), err)
	cfgBytes := b.Bytes()
	jobmaster, err := registry.GlobalWorkerRegistry().CreateWorker(dctx, lib.DMJobMaster, "dm-jobmaster", libMetadata.JobManagerUUID, cfgBytes)
	require.NoError(t.T(), err)

	// Init
	require.NoError(t.T(), jobmaster.Init(context.Background()))

	// mock master failed and recoverd after init
	dctx = dcontext.Background()
	dp = deps.NewDeps()
	require.NoError(t.T(), dp.Provide(func() masterParamListForTest {
		return depsForTest
	}))
	dctx = dctx.WithDeps(dp)
	require.NoError(t.T(), jobmaster.Close(context.Background()))
	jobmaster, err = registry.GlobalWorkerRegistry().CreateWorker(dctx, lib.DMJobMaster, "dm-jobmaster", libMetadata.JobManagerUUID, cfgBytes)
	require.NoError(t.T(), err)
	require.NoError(t.T(), jobmaster.Init(context.Background()))

	// Poll
	mockServerMasterClient.On("ScheduleTask", mock.Anything, mock.Anything, mock.Anything).
		Return(&pb.TaskSchedulerResponse{Schedule: map[int64]*pb.ScheduleResult{0: {}}}, nil).Once()
	mockServerMasterClient.On("ScheduleTask", mock.Anything, mock.Anything, mock.Anything).
		Return(&pb.TaskSchedulerResponse{Schedule: map[int64]*pb.ScheduleResult{0: {}}}, nil).Once()
	require.NoError(t.T(), jobmaster.Poll(context.Background()))
	require.NoError(t.T(), jobmaster.Poll(context.Background()))
	require.NoError(t.T(), jobmaster.Poll(context.Background()))

	// Close
	require.NoError(t.T(), jobmaster.Close(context.Background()))
}

func (t *testDMJobmasterSuite) TestDMJobmaster() {
	metaKVClient := kvmock.NewMetaMock()
	mockBaseJobmaster := &MockBaseJobmaster{}
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jm := &JobMaster{
		workerID:              "jobmaster-id",
		jobCfg:                jobCfg,
		closeCh:               make(chan struct{}),
		messageHandlerManager: p2p.NewMockMessageHandlerManager(),
		BaseJobMaster:         mockBaseJobmaster,
	}

	// init
	mockBaseJobmaster.On("FrameMetaClient").Return(metaKVClient)
	mockBaseJobmaster.On("GetWorkers").Return(map[string]lib.WorkerHandle{}).Once()
	require.NoError(t.T(), jm.InitImpl(context.Background()))
	// no error if init again
	// though frame ensures that it will not init twice
	mockBaseJobmaster.On("GetWorkers").Return(map[string]lib.WorkerHandle{}).Once()
	require.NoError(t.T(), jm.InitImpl(context.Background()))

	// recover
	jm = &JobMaster{
		workerID:              "jobmaster-id",
		jobCfg:                jobCfg,
		closeCh:               make(chan struct{}),
		messageHandlerManager: p2p.NewMockMessageHandlerManager(),
		BaseJobMaster:         mockBaseJobmaster,
	}
	mockBaseJobmaster.On("GetWorkers").Return(map[string]lib.WorkerHandle{}).Once()
	jm.OnMasterRecovered(context.Background())

	// tick
	worker1 := "worker1"
	worker2 := "worker2"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker1, nil).Once()
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker2, nil).Once()
	require.NoError(t.T(), jm.Tick(context.Background()))
	require.NoError(t.T(), jm.Tick(context.Background()))
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker1 online, worker2 dispatch error
	workerHandle1 := &lib.MockWorkerHandler{WorkerID: worker1}
	workerHandle2 := &lib.MockWorkerHandler{WorkerID: worker2}
	task1 := jobCfg.Upstreams[0].SourceID
	taskStatus1 := runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  task1,
			Stage: metadata.StageRunning,
		},
	}
	bytes1, err := json.Marshal(taskStatus1)
	require.NoError(t.T(), err)
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false)
	jm.OnWorkerOnline(workerHandle1)
	jm.OnWorkerDispatched(workerHandle2, errors.New("dispatch error"))
	worker3 := "worker3"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker3, nil).Once()
	workerHandle1.On("SendMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker1 offline, worker3 online
	workerHandle2.WorkerID = worker3
	task2 := jobCfg.Upstreams[1].SourceID
	taskStatus2 := runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  task2,
			Stage: metadata.StageRunning,
		},
	}
	bytes2, err := json.Marshal(taskStatus2)
	require.NoError(t.T(), err)
	workerHandle2.On("IsTombStone").Return(false)
	workerHandle2.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes2}).Once()
	jm.OnWorkerOnline(workerHandle2)
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	jm.OnWorkerOffline(workerHandle1, errors.New("offline error"))
	worker4 := "worker4"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker4, nil).Once()
	workerHandle2.On("SendMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker4 online
	workerHandle1.WorkerID = worker4
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	jm.OnWorkerOnline(workerHandle1)
	require.NoError(t.T(), jm.Tick(context.Background()))

	// master failover
	jm = &JobMaster{
		workerID:              "jobmaster-id",
		jobCfg:                jobCfg,
		closeCh:               make(chan struct{}),
		messageHandlerManager: p2p.NewMockMessageHandlerManager(),
		BaseJobMaster:         mockBaseJobmaster,
	}
	mockBaseJobmaster.On("GetWorkers").Return(map[string]lib.WorkerHandle{worker4: workerHandle1, worker3: workerHandle2}).Once()
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle2.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes2}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	workerHandle2.On("IsTombStone").Return(false).Once()
	workerHandle1.On("SendMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	workerHandle2.On("SendMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	jm.OnMasterRecovered(context.Background())
	require.NoError(t.T(), jm.Tick(context.Background()))
	// placeholder
	require.NoError(t.T(), jm.OnWorkerStatusUpdated(workerHandle1, &libModel.WorkerStatus{ExtBytes: bytes1}))
	require.NoError(t.T(), jm.OnJobManagerMessage("", ""))
	require.NoError(t.T(), jm.OnMasterMessage("", ""))
	require.NoError(t.T(), jm.OnJobManagerFailover(lib.MasterFailoverReason{}))
	require.NoError(t.T(), jm.OnMasterFailover(lib.MasterFailoverReason{}))
	require.Equal(t.T(), jm.Workload(), model.RescUnit(2))
	require.NoError(t.T(), jm.OnWorkerStatusUpdated(workerHandle1, &libModel.WorkerStatus{ExtBytes: bytes1}))
	require.EqualError(t.T(), jm.OnWorkerMessage(workerHandle1, "", dmpkg.MessageWithID{}), "request 0 not found")

	// Close
	workerHandle1.On("SendMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	workerHandle2.On("SendMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), jm.CloseImpl(context.Background()))
	}()
	time.Sleep(time.Second)
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle2.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes2}).Once()
	jm.OnWorkerOffline(workerHandle1, errors.New("offline error"))
	jm.OnWorkerOffline(workerHandle2, errors.New("offline error"))
	wg.Wait()
}

// TODO: move to separate file
type MockBaseJobmaster struct {
	mu sync.Mutex
	mock.Mock

	lib.BaseJobMaster
	metaKVClient metaclient.KVClient
}

func (m *MockBaseJobmaster) JobMasterID() libModel.MasterID {
	return libMetadata.JobManagerUUID
}

func (m *MockBaseJobmaster) GetWorkers() map[string]lib.WorkerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(map[string]lib.WorkerHandle)
}

func (m *MockBaseJobmaster) FrameMetaClient() metaclient.KVClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(metaclient.KVClient)
}

func (m *MockBaseJobmaster) CreateWorker(workerType lib.WorkerType, config lib.WorkerConfig, cost model.RescUnit) (libModel.WorkerID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(libModel.WorkerID), args.Error(1)
}

func (m *MockBaseJobmaster) CurrentEpoch() int64 {
	return 0
}
