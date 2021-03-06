package dm

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/dm/checker"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	libMetadata "github.com/hanfei1991/microcosm/lib/metadata"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	extkv "github.com/hanfei1991/microcosm/pkg/meta/extension"
	kvmock "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
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
	taskNormalInterval = time.Hour
	taskErrorInterval = 100 * time.Millisecond
	WorkerNormalInterval = time.Hour
	WorkerErrorInterval = 100 * time.Millisecond
	runtime.HeartbeatInterval = 1 * time.Second
	require.NoError(t.T(), log.InitLogger(&log.Config{Level: "debug"}))
}

type masterParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	UserRawKVClient       extkv.KVClientEx
	ExecutorClientManager client.ClientsManager
	ServerMasterClient    client.MasterClient
	ResourceBroker        broker.Broker
}

// Init -> Poll -> Close
func (t *testDMJobmasterSuite) TestRunDMJobMaster() {
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t.T(), err)
	mockServerMasterClient := &client.MockServerMasterClient{}
	mockExecutorClient := client.NewClientManager()
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
	cfgBytes, err := ioutil.ReadFile(jobTemplatePath)
	require.NoError(t.T(), err)
	jobmaster, err := registry.GlobalWorkerRegistry().CreateWorker(dctx, lib.DMJobMaster, "dm-jobmaster", libMetadata.JobManagerUUID, cfgBytes)
	require.NoError(t.T(), err)
	// Init
	_, mockDB, err := conn.InitMockDBFull()
	require.NoError(t.T(), err)
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "check pass", nil
	}
	require.NoError(t.T(), jobmaster.Init(context.Background()))

	// mock master failed and recoverd after init
	require.NoError(t.T(), jobmaster.Close(context.Background()))

	jobmaster, err = registry.GlobalWorkerRegistry().CreateWorker(dctx, lib.DMJobMaster, "dm-jobmaster", libMetadata.JobManagerUUID, cfgBytes)
	require.NoError(t.T(), err)
	_, mockDB, err = conn.InitMockDBFull()
	require.NoError(t.T(), err)
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t.T(), jobmaster.Init(context.Background()))

	// Poll
	mockServerMasterClient.On("ScheduleTask", mock.Anything, mock.Anything, mock.Anything).
		Return(&pb.ScheduleTaskResponse{}, nil).Once()
	mockServerMasterClient.On("ScheduleTask", mock.Anything, mock.Anything, mock.Anything).
		Return(&pb.ScheduleTaskResponse{}, nil).Once()
	require.NoError(t.T(), jobmaster.Poll(context.Background()))
	require.NoError(t.T(), jobmaster.Poll(context.Background()))
	require.NoError(t.T(), jobmaster.Poll(context.Background()))

	// Close
	require.NoError(t.T(), jobmaster.Close(context.Background()))
}

func (t *testDMJobmasterSuite) TestDMJobmaster() {
	metaKVClient := kvmock.NewMetaMock()
	mockBaseJobmaster := &MockBaseJobmaster{}
	mockCheckpointAgent := &MockCheckpointAgent{}
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jm := &JobMaster{
		workerID:              "jobmaster-id",
		jobCfg:                jobCfg,
		closeCh:               make(chan struct{}),
		messageHandlerManager: p2p.NewMockMessageHandlerManager(),
		BaseJobMaster:         mockBaseJobmaster,
		checkpointAgent:       mockCheckpointAgent,
	}

	// init
	precheckError := errors.New("precheck error")
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "", precheckError
	}
	mockBaseJobmaster.On("MetaKVClient").Return(metaKVClient)
	mockBaseJobmaster.On("GetWorkers").Return(map[string]lib.WorkerHandle{}).Once()
	require.EqualError(t.T(), jm.InitImpl(context.Background()), precheckError.Error())

	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "check pass", nil
	}
	mockBaseJobmaster.On("MetaKVClient").Return(metaKVClient)
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
		checkpointAgent:       mockCheckpointAgent,
	}
	mockBaseJobmaster.On("GetWorkers").Return(map[string]lib.WorkerHandle{}).Once()
	jm.OnMasterRecovered(context.Background())

	// tick
	worker1 := "worker1"
	worker2 := "worker2"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker1, nil).Once()
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker2, nil).Once()
	mockCheckpointAgent.On("IsFresh", mock.Anything).Return(true, nil).Times(6)
	require.NoError(t.T(), jm.Tick(context.Background()))
	require.NoError(t.T(), jm.Tick(context.Background()))
	require.NoError(t.T(), jm.Tick(context.Background()))
	// make sure workerHandle1 bound to task status1, workerHandle2 bound to task status2.
	taskStatus1 := runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Stage: metadata.StageRunning,
		},
	}
	taskStatus2 := runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Stage: metadata.StageRunning,
		},
	}
	jm.workerManager.workerStatusMap.Range(func(key, val interface{}) bool {
		if val.(runtime.WorkerStatus).ID == worker1 {
			taskStatus1.Task = key.(string)
		} else {
			taskStatus2.Task = key.(string)
		}
		return true
	})
	workerHandle1 := &lib.MockWorkerHandler{WorkerID: worker1}
	workerHandle2 := &lib.MockWorkerHandler{WorkerID: worker2}

	// worker1 online, worker2 dispatch error
	bytes1, err := json.Marshal(taskStatus1)
	require.NoError(t.T(), err)
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	jm.OnWorkerOnline(workerHandle1)
	jm.OnWorkerDispatched(workerHandle2, errors.New("dispatch error"))
	worker3 := "worker3"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker3, nil).Once()
	mockCheckpointAgent.On("IsFresh", mock.Anything).Return(true, nil).Times(3)
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker1 offline, worker3 online
	workerHandle2.WorkerID = worker3

	bytes2, err := json.Marshal(taskStatus2)
	require.NoError(t.T(), err)
	workerHandle2.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes2}).Once()
	workerHandle2.On("IsTombStone").Return(false).Once()
	jm.OnWorkerOnline(workerHandle2)
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	jm.OnWorkerOffline(workerHandle1, errors.New("offline error"))
	worker4 := "worker4"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker4, nil).Once()
	mockCheckpointAgent.On("IsFresh", mock.Anything).Return(true, nil).Times(3)
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker4 online
	workerHandle1.WorkerID = worker4
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	jm.OnWorkerOnline(workerHandle1)
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker1 finished
	taskStatus1.Stage = metadata.StageFinished
	bytes1, err = json.Marshal(taskStatus1)
	require.NoError(t.T(), err)
	worker5 := "worker5"
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker5, nil).Once()
	jm.OnWorkerOffline(workerHandle1, nil)
	require.NoError(t.T(), jm.Tick(context.Background()))
	// worker5 online
	workerHandle1.WorkerID = worker5
	taskStatus1.Stage = metadata.StageRunning
	bytes1, err = json.Marshal(taskStatus1)
	require.NoError(t.T(), err)
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	jm.OnWorkerOnline(workerHandle1)
	require.NoError(t.T(), jm.Tick(context.Background()))

	// master failover
	jm = &JobMaster{
		workerID:              "jobmaster-id",
		jobCfg:                jobCfg,
		closeCh:               make(chan struct{}),
		messageHandlerManager: p2p.NewMockMessageHandlerManager(),
		BaseJobMaster:         mockBaseJobmaster,
		checkpointAgent:       mockCheckpointAgent,
	}
	mockBaseJobmaster.On("GetWorkers").Return(map[string]lib.WorkerHandle{worker4: workerHandle1, worker3: workerHandle2}).Once()
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle2.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes2}).Once()
	workerHandle1.On("IsTombStone").Return(false).Twice()
	workerHandle2.On("IsTombStone").Return(false).Twice()
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
	time.Sleep(time.Second * 2)
	workerHandle1.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle2.On("Status").Return(&libModel.WorkerStatus{ExtBytes: bytes2}).Once()
	jm.OnWorkerOffline(workerHandle1, errors.New("offline error"))
	jm.OnWorkerOffline(workerHandle2, errors.New("offline error"))
	wg.Wait()

	mockBaseJobmaster.AssertExpectations(t.T())
	workerHandle1.AssertExpectations(t.T())
	workerHandle2.AssertExpectations(t.T())
	mockCheckpointAgent.AssertExpectations(t.T())
}

// TODO: move to separate file
type MockBaseJobmaster struct {
	mu sync.Mutex
	mock.Mock

	lib.BaseJobMaster
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

func (m *MockBaseJobmaster) MetaKVClient() metaclient.KVClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(metaclient.KVClient)
}

func (m *MockBaseJobmaster) CreateWorker(workerType lib.WorkerType, config lib.WorkerConfig, cost model.RescUnit, resources ...resourcemeta.ResourceID) (libModel.WorkerID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(libModel.WorkerID), args.Error(1)
}

func (m *MockBaseJobmaster) CurrentEpoch() int64 {
	return 0
}

type MockCheckpointAgent struct {
	mu sync.Mutex
	mock.Mock
}

func (m *MockCheckpointAgent) Init(ctx context.Context) error {
	return nil
}

func (m *MockCheckpointAgent) Remove(ctx context.Context) error {
	return nil
}

func (m *MockCheckpointAgent) IsFresh(ctx context.Context, workerType lib.WorkerType, taskCfg *metadata.Task) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(bool), args.Error(1)
}
