package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/client"
	runtime "github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib/quota"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
	extKV "github.com/hanfei1991/microcosm/pkg/metaclient/extention"
	"github.com/hanfei1991/microcosm/pkg/metaclient/kvclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

type Master interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	MasterID() MasterID

	runtime.Closer
}

type MasterImpl interface {
	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval.
	Tick(ctx context.Context) error

	// OnMasterRecovered is called when the master has recovered from an error.
	OnMasterRecovered(ctx context.Context) error

	// OnWorkerDispatched is called when a request to launch a worker is finished.
	OnWorkerDispatched(worker WorkerHandle, result error) error

	// OnWorkerOnline is called when the first heartbeat for a worker is received.
	OnWorkerOnline(worker WorkerHandle) error

	// OnWorkerOffline is called when a worker exits or has timed out.
	OnWorkerOffline(worker WorkerHandle, reason error) error

	// OnWorkerMessage is called when a customized message is received.
	OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error

	// CloseImpl is called when the master is being closed
	CloseImpl(ctx context.Context) error
}

const (
	createWorkerTimeout        = 10 * time.Second
	maxCreateWorkerConcurrency = 100
)

type BaseMaster interface {
	// MetaKVClient return user metastore kv client
	MetaKVClient() metaclient.KVClient
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	MasterMeta() *MasterMetaKVData
	MasterID() MasterID
	GetWorkers() map[WorkerID]WorkerHandle
	IsMasterReady() bool
	Close(ctx context.Context) error
	OnError(err error)
	// CreateWorker registers worker handler and dispatches worker to executor
	CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error)
}

type DefaultBaseMaster struct {
	Impl MasterImpl

	// dependencies
	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	// frame metastore prefix kvclient
	metaKVClient metaclient.KVClient
	// user metastore raw kvclient
	userRawKVClient       extKV.KVClientEx
	executorClientManager client.ClientsManager
	serverMasterClient    client.MasterClient
	pool                  workerpool.AsyncPool

	clock clock.Clock

	// workerManager maintains the list of all workers and
	// their statuses.
	workerManager workerManager

	currentEpoch atomic.Int64

	wg    sync.WaitGroup
	errCh chan error

	// closeCh is closed when the BaseMaster is exiting
	closeCh chan struct{}

	id            MasterID // id of this master itself
	advertiseAddr string
	nodeID        p2p.NodeID
	timeoutConfig TimeoutConfig
	masterMeta    *MasterMetaKVData

	// user metastore prefix kvclient
	// Don't close it. It's just a prefix wrapper for underlying userRawKVClient
	userMetaKVClient metaclient.KVClient

	// components for easier unit testing
	uuidGen uuid.Generator

	// TODO use a shared quota for all masters.
	createWorkerQuota quota.ConcurrencyQuota

	// deps is a container for injected dependencies
	deps *deps.Deps
}

type masterParams struct {
	dig.In

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	// frame metastore prefix kvclient
	MetaKVClient metaclient.KVClient
	// user metastore raw kvclient
	UserRawKVClient       extKV.KVClientEx
	ExecutorClientManager client.ClientsManager
	ServerMasterClient    client.MasterClient
}

func NewBaseMaster(
	ctx *dcontext.Context,
	impl MasterImpl,
	id MasterID,
) BaseMaster {
	var (
		nodeID        p2p.NodeID
		advertiseAddr string
		masterMeta    = &MasterMetaKVData{}
		params        masterParams
	)
	if ctx != nil {
		nodeID = ctx.Environ.NodeID
		advertiseAddr = ctx.Environ.Addr
		metaBytes := ctx.Environ.MasterMetaBytes
		err := masterMeta.Unmarshal(metaBytes)
		if err != nil {
			log.L().Warn("invalid master meta", zap.ByteString("data", metaBytes), zap.Error(err))
		}
	}

	if err := ctx.Deps().Fill(&params); err != nil {
		// TODO more elegant error handling
		log.L().Panic("failed to provide dependencies", zap.Error(err))
	}

	return &DefaultBaseMaster{
		Impl:                  impl,
		messageHandlerManager: params.MessageHandlerManager,
		messageSender:         params.MessageSender,
		metaKVClient:          params.MetaKVClient,
		userRawKVClient:       params.UserRawKVClient,
		executorClientManager: params.ExecutorClientManager,
		serverMasterClient:    params.ServerMasterClient,
		pool:                  workerpool.NewDefaultAsyncPool(4),
		id:                    id,
		clock:                 clock.New(),

		timeoutConfig: defaultTimeoutConfig,
		masterMeta:    masterMeta,

		errCh:   make(chan error, 1),
		closeCh: make(chan struct{}),

		uuidGen: uuid.NewGenerator(),

		nodeID:        nodeID,
		advertiseAddr: advertiseAddr,

		createWorkerQuota: quota.NewConcurrencyQuota(maxCreateWorkerConcurrency),
		// [TODO] use tenantID if support muliti-tenant
		userMetaKVClient: kvclient.NewPrefixKVClient(params.UserRawKVClient, tenant.DefaultUserTenantID),
		deps:             ctx.Deps(),
	}
}

func (m *DefaultBaseMaster) MetaKVClient() metaclient.KVClient {
	return m.userMetaKVClient
}

func (m *DefaultBaseMaster) Init(ctx context.Context) error {
	isInit, err := m.doInit(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	if isInit {
		if err := m.Impl.InitImpl(ctx); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err := m.Impl.OnMasterRecovered(ctx); err != nil {
			return errors.Trace(err)
		}
	}

	if err := m.markStatusCodeInMetadata(ctx, MasterStatusInit); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *DefaultBaseMaster) doInit(ctx context.Context) (isFirstStartUp bool, err error) {
	if err := m.registerMessageHandlers(ctx); err != nil {
		return false, errors.Trace(err)
	}

	isInit, epoch, err := m.refreshMetadata(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	m.currentEpoch.Store(epoch)

	if err := m.deps.Provide(func() workerpool.AsyncPool {
		return m.pool
	}); err != nil {
		return false, errors.Trace(err)
	}

	// TODO refactor context use when we can replace stdContext with dcontext.
	dctx := dcontext.NewContext(ctx, log.L()).WithDeps(m.deps)
	m.workerManager = newWorkerManager(
		dctx,
		m.id,
		!isInit,
		epoch,
		&m.timeoutConfig)

	m.startBackgroundTasks()
	return isInit, nil
}

func (m *DefaultBaseMaster) registerMessageHandlers(ctx context.Context) error {
	ok, err := m.messageHandlerManager.RegisterHandler(
		ctx,
		HeartbeatPingTopic(m.id),
		&HeartbeatPingMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*HeartbeatPingMessage)
			if err := m.workerManager.HandleHeartbeat(msg, sender); err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	if err != nil {
		return err
	}
	if !ok {
		log.L().Panic("duplicate handler", zap.String("topic", HeartbeatPingTopic(m.id)))
	}

	ok, err = m.messageHandlerManager.RegisterHandler(
		ctx,
		WorkerStatusUpdatedTopic(m.id),
		&WorkerStatusUpdatedMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*WorkerStatusUpdatedMessage)
			m.workerManager.OnWorkerStatusUpdated(msg)
			return nil
		})
	if err != nil {
		return err
	}
	if !ok {
		log.L().Panic("duplicate handler", zap.String("topic", WorkerStatusUpdatedTopic(m.id)))
	}
	return nil
}

func (m *DefaultBaseMaster) Poll(ctx context.Context) error {
	if err := m.doPoll(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := m.Impl.Tick(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *DefaultBaseMaster) doPoll(ctx context.Context) error {
	select {
	case err := <-m.errCh:
		if err != nil {
			return errors.Trace(err)
		}
	case <-m.closeCh:
		return derror.ErrMasterClosed.GenWithStackByArgs()
	default:
	}

	if err := m.messageHandlerManager.CheckError(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := m.workerManager.CheckStatusUpdate(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *DefaultBaseMaster) MasterMeta() *MasterMetaKVData {
	return m.masterMeta
}

func (m *DefaultBaseMaster) MasterID() MasterID {
	return m.id
}

func (m *DefaultBaseMaster) GetWorkers() map[WorkerID]WorkerHandle {
	return m.workerManager.GetWorkers()
}

func (m *DefaultBaseMaster) doClose() {
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	close(m.closeCh)
	m.wg.Wait()
	if err := m.messageHandlerManager.Clean(closeCtx); err != nil {
		log.L().Warn("Failed to clean up message handlers",
			zap.String("master-id", m.id))
	}
}

func (m *DefaultBaseMaster) Close(ctx context.Context) error {
	if err := m.Impl.CloseImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	m.doClose()
	return nil
}

func (m *DefaultBaseMaster) startBackgroundTasks() {
	cctx, cancel := context.WithCancel(context.Background())
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		<-m.closeCh
		cancel()
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.pool.Run(cctx); err != nil {
			m.OnError(err)
		}
	}()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.runWorkerCheck(cctx); err != nil {
			m.OnError(err)
		}
	}()
}

func (m *DefaultBaseMaster) runWorkerCheck(ctx context.Context) error {
	ticker := time.NewTicker(m.timeoutConfig.masterHeartbeatCheckLoopInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		offlinedWorkers, onlinedWorkers := m.workerManager.Tick(ctx)
		// It is logical to call `OnWorkerOnline` first and then call `OnWorkerOffline`.
		// In case that these two events for the same worker is detected in the same tick.
		for _, workerInfo := range onlinedWorkers {
			log.L().Info("worker is online", zap.Any("worker-info", workerInfo))

			handle := m.workerManager.GetWorkerHandle(workerInfo.ID)
			err := m.Impl.OnWorkerOnline(handle)
			if err != nil {
				return errors.Trace(err)
			}
		}

		for _, workerInfo := range offlinedWorkers {
			status, ok := m.workerManager.GetStatus(workerInfo.ID)
			if !ok {
				log.L().Panic(
					"offlined worker has no status found",
					zap.Any("worker-info", workerInfo),
				)
			}
			log.L().Info("worker is offline", zap.Any("master-id", m.id), zap.Any("worker-info", workerInfo), zap.Any("work-status", status))
			tombstoneHandle := NewTombstoneWorkerHandle(workerInfo.ID, *status, nil)
			var offlineError error
			if status.Code == WorkerStatusFinished {
				offlineError = derror.ErrWorkerFinish.FastGenByArgs()
			} else {
				offlineError = derror.ErrWorkerOffline.FastGenByArgs(workerInfo.ID)
			}

			err := m.Impl.OnWorkerOffline(tombstoneHandle, offlineError)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (m *DefaultBaseMaster) OnError(err error) {
	if errors.Cause(err) == context.Canceled {
		// TODO think about how to gracefully handle cancellation here.
		log.L().Warn("BaseMaster is being canceled", zap.String("id", m.id), zap.Error(err))
		return
	}
	select {
	case m.errCh <- err:
	default:
	}
}

// refreshMetadata load and update metadata by current nodeID, advertiseAddr, etc.
// master meta is persisted before it is created, in this function we update some
// fileds to the current value, including nodeID and advertiseAddr.
func (m *DefaultBaseMaster) refreshMetadata(ctx context.Context) (isInit bool, epoch Epoch, err error) {
	metaClient := NewMasterMetadataClient(m.id, m.metaKVClient)

	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return false, 0, err
	}
	epoch = masterMeta.Epoch

	// We should update the master data to reflect our current information
	masterMeta.Addr = m.advertiseAddr
	masterMeta.NodeID = m.nodeID

	if err := metaClient.Store(ctx, masterMeta); err != nil {
		return false, 0, errors.Trace(err)
	}

	m.masterMeta = masterMeta
	// isInit true means the master is created but has not been initialized.
	isInit = masterMeta.StatusCode == MasterStatusUninit

	return
}

func (m *DefaultBaseMaster) markStatusCodeInMetadata(
	ctx context.Context, code MasterStatusCode,
) error {
	metaClient := NewMasterMetadataClient(m.id, m.metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	masterMeta.StatusCode = code
	return metaClient.Store(ctx, masterMeta)
}

// prepareWorkerConfig extracts information from WorkerConfig into detail fields.
// - If workerType is master type, the config is a `*MasterMetaKVData` struct and
//   contains pre allocated maseter ID, and json marshalled config.
// - If workerType is worker type, the config is a user defined config struct, we
//   marshal it to byte slice as returned config, and generate a random WorkerID.
func (m *DefaultBaseMaster) prepareWorkerConfig(
	workerType WorkerType, config WorkerConfig,
) (rawConfig []byte, workerID string, err error) {
	switch workerType {
	case CvsJobMaster, FakeJobMaster, DMJobMaster:
		masterMeta, ok := config.(*MasterMetaKVData)
		if !ok {
			err = derror.ErrMasterInvalidMeta.GenWithStackByArgs(config)
			return
		}
		rawConfig = masterMeta.Config
		workerID = masterMeta.ID
	case WorkerDMDump, WorkerDMLoad, WorkerDMSync:
		var b bytes.Buffer
		err = toml.NewEncoder(&b).Encode(config)
		if err != nil {
			return
		}
		rawConfig = b.Bytes()
		workerID = m.uuidGen.NewString()
	default:
		rawConfig, err = json.Marshal(config)
		if err != nil {
			return
		}
		workerID = m.uuidGen.NewString()
	}
	return
}

func (m *DefaultBaseMaster) CreateWorker(workerType WorkerType, config WorkerConfig, cost model.RescUnit) (WorkerID, error) {
	log.L().Info("CreateWorker",
		zap.Int64("worker-type", int64(workerType)),
		zap.Any("worker-config", config),
		zap.String("master-id", m.id))

	if !m.createWorkerQuota.TryConsume() {
		return "", derror.ErrMasterConcurrencyExceeded.GenWithStackByArgs()
	}

	configBytes, workerID, err := m.prepareWorkerConfig(workerType, config)
	if err != nil {
		return "", err
	}

	go func() {
		// TODO make the timeout configurable
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		defer func() {
			m.createWorkerQuota.Release()
		}()

		// When CreateWorker failed, we need to pass the worker id to
		// OnWorkerDispatched, so we use a dummy WorkerHandle.
		dispatchFailedDummyHandler := NewTombstoneWorkerHandle(
			workerID, WorkerStatus{Code: WorkerStatusError}, nil)
		requestCtx, cancel := context.WithTimeout(context.Background(), createWorkerTimeout)
		defer cancel()
		// This following API should be refined.
		resp, err := m.serverMasterClient.ScheduleTask(requestCtx, &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
			Task: &pb.TaskRequest{
				Id: 0,
			},
			Cost: int64(cost),
		}}},
			// TODO (zixiong) make the timeout configurable
			time.Second*10)
		if err != nil {
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}
		schedule := resp.GetSchedule()
		if len(schedule) != 1 {
			log.L().Panic("unexpected schedule result", zap.Any("schedule", schedule))
		}
		executorID := model.ExecutorID(schedule[0].ExecutorId)

		err = m.executorClientManager.AddExecutor(executorID, schedule[0].Addr)
		if err != nil {
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}
		executorClient := m.executorClientManager.ExecutorClient(executorID)
		executorResp, err := executorClient.Send(requestCtx, &client.ExecutorRequest{
			Cmd: client.CmdDispatchTask,
			Req: &pb.DispatchTaskRequest{
				TaskTypeId: int64(workerType),
				TaskConfig: configBytes,
				MasterId:   m.id,
				WorkerId:   workerID,
			},
		})
		if err != nil {
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}
		dispatchTaskResp := executorResp.Resp.(*pb.DispatchTaskResponse)
		log.L().Info("Worker dispatched", zap.Any("master-id", m.id), zap.Any("response", dispatchTaskResp))
		errCode := dispatchTaskResp.GetErrorCode()
		if errCode != pb.DispatchTaskErrorCode_OK {
			err1 := m.Impl.OnWorkerDispatched(dispatchFailedDummyHandler,
				errors.Errorf("dispatch worker failed with error code: %d", errCode))
			if err1 != nil {
				m.OnError(errors.Trace(err1))
			}
			return
		}

		if err := m.workerManager.OnWorkerCreated(ctx, workerID, p2p.NodeID(executorID)); err != nil {
			m.OnError(errors.Trace(err))
		}
		handle := m.workerManager.GetWorkerHandle(workerID)

		if err := m.Impl.OnWorkerDispatched(handle, nil); err != nil {
			m.OnError(errors.Trace(err))
		}
	}()

	return workerID, nil
}

func (m *DefaultBaseMaster) IsMasterReady() bool {
	return m.workerManager.IsInitialized()
}
