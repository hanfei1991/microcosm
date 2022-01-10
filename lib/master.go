package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Master interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() MasterID
	Close(ctx context.Context) error
}

type MasterImpl interface {
	Master

	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval.
	Tick(ctx context.Context) error

	// OnWorkerDispatched is called when a request to launch a worker is finished.
	OnWorkerDispatched(worker WorkerHandle, result error) error

	// OnWorkerOnline is called when the first heartbeat for a worker is received.
	OnWorkerOnline(worker WorkerHandle) error

	// OnWorkerOffline is called when a worker exits or has timed out.
	OnWorkerOffline(worker WorkerHandle, reason error) error

	// OnWorkerMessage is called when a customized message is received.
	OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error

	// OnMetadataInit is called when the master is initialized and the metadata might
	// need to be checked and fixed.
	OnMetadataInit(etx interface{}) (interface{}, error)

	// CloseImpl is called when the master is being closed
	CloseImpl(ctx context.Context) error
}

type BaseMaster struct {
	MasterImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageRouter         p2p.MessageRouter
	metaKVClient          metadata.MetaKV
	executorClientManager *client.Manager
	serverMasterClient    *client.MasterClient
	metadataManager       MasterMetadataManager

	workers *workerManager

	pool workerpool.AsyncPool

	// read-only fields
	currentEpoch atomic.Int64
	id           MasterID

	wg    sync.WaitGroup
	errCh chan error
}

func NewBaseMaster(
	ctx context.Context,
	impl MasterImpl,
	id MasterID,
	messageHandlerManager p2p.MessageHandlerManager,
	messageRouter p2p.MessageRouter,
	metaKVClient metadata.MetaKV,
	executorClientManager *client.Manager,
	serverMasterClient *client.MasterClient,
	metadataManager MasterMetadataManager,
) *BaseMaster {
	return &BaseMaster{
		MasterImpl:            impl,
		messageHandlerManager: messageHandlerManager,
		messageRouter:         messageRouter,
		metaKVClient:          metaKVClient,
		executorClientManager: executorClientManager,
		serverMasterClient:    serverMasterClient,
		metadataManager:       metadataManager,

		pool: workerpool.NewDefaultAsyncPool(4),
		id:   id,
	}
}

func (m *BaseMaster) Init(ctx context.Context) error {
	m.startBackgroundTasks(ctx)

	if err := m.initMessageHandlers(ctx); err != nil {
		return errors.Trace(err)
	}

	isInit, epoch, err := m.metadataManager.FixUpAndInit(ctx, m.OnMetadataInit)
	if err != nil {
		return errors.Trace(err)
	}
	m.currentEpoch.Store(epoch)
	m.workers = newWorkerManager(m.id, !isInit, epoch)
	if err := m.InitImpl(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *BaseMaster) InternalID() MasterID {
	return m.id
}

func (m *BaseMaster) startBackgroundTasks(ctx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.pool.Run(ctx); err != nil {
			m.OnError(err)
		}
	}()
}

func (m *BaseMaster) OnError(err error) {
	select {
	case m.errCh <- err:
	default:
	}
}

func (m *BaseMaster) initMessageHandlers(ctx context.Context) error {
	topic := HeartbeatPingTopic(m.id)
	ok, err := m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&HeartbeatPingMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			heartBeatMsg := value.(*HeartbeatPingMessage)
			curEpoch := m.currentEpoch.Load()
			if heartBeatMsg.Epoch < curEpoch {
				log.L().Info("stale message dropped",
					zap.Any("message", heartBeatMsg),
					zap.Int64("cur-epoch", curEpoch))
				return nil
			}
			m.workers.HandleHeartBeat(heartBeatMsg)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handler",
			zap.String("topic", topic))
	}

	topic = WorkloadReportTopic(m.id)
	ok, err = m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&WorkloadReportMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			workloadMessage := value.(*WorkloadReportMessage)
			m.workers.UpdateWorkload(workloadMessage)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handler",
			zap.String("topic", topic))
	}

	topic = StatusUpdateTopic(m.id)
	ok, err = m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&StatusUpdateMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			statusUpdateMessage := value.(*StatusUpdateMessage)
			m.workers.UpdateStatus(statusUpdateMessage)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handler",
			zap.String("topic", topic))
	}
	return nil
}

func (m *BaseMaster) CreateWorker(ctx context.Context, workerType WorkerType, config []byte) error {
	err := m.pool.Go(ctx, func() {
		// This following API should be refined.
		resp, err := m.serverMasterClient.ScheduleTask(ctx, &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
			Task: &pb.TaskRequest{
				Id: 0,
			},
			// TODO (zixiong) implement the real cost.
			Cost: 10,
		}}},
			// TODO (zixiong) make the timeout configurable
			time.Second*10)
		if err != nil {
			err1 := m.OnWorkerDispatched(nil, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err))
			}
			return
		}
		schedule := resp.GetSchedule()
		if len(schedule) != 1 {
			panic("unreachable")
		}
		executorID := schedule[0].ExecutorId

		executorClient := m.executorClientManager.ExecutorClient(model.ExecutorID(executorID))
		executorResp, err := executorClient.Send(ctx, &client.ExecutorRequest{
			Cmd: client.CmdDispatchTask,
			Req: &pb.DispatchTaskRequest{
				TaskTypeId: int64(workerType),
				TaskConfig: config,
			},
		})
		if err != nil {
			err1 := m.OnWorkerDispatched(nil, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err))
			}
			return
		}
		dispatchTaskResp := executorResp.Resp.(*pb.DispatchTaskResponse)
		errCode := dispatchTaskResp.GetErrorCode()
		if errCode != pb.DispatchTaskErrorCode_OK {
			err1 := m.OnWorkerDispatched(
				nil, errors.Errorf("dispatch worker failed with error code: %d", errCode))
			if err1 != nil {
				m.OnError(errors.Trace(err))
			}
			return
		}

		workerID := WorkerID(dispatchTaskResp.GetWorkerId())
		m.workers.onWorkerCreated(workerID, executorID)
		handle := m.workers.getWorkerHandle(workerID)

		if err := m.OnWorkerDispatched(handle, nil); err != nil {
			m.OnError(errors.Trace(err))
		}
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// workerManager is for private use by BaseMaster.
type workerManager struct {
	mu                   sync.Mutex
	needWaitForHeartBeat bool
	initStartTime        time.Time
	workerInfos          map[WorkerID]*WorkerInfo

	// read-only
	masterEpoch epoch
	masterID    MasterID

	messageRouter p2p.MessageRouter
}

type WorkerHandle interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error
	Status() *WorkerStatus
	Workload() model.RescUnit
}

type workerHandleImpl struct {
	manager  *workerManager
	workerID WorkerID
}

func (w *workerHandleImpl) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error {
	info, ok := w.manager.getWorkerInfo(w.workerID)
	if !ok {
		return derror.ErrWorkerNotFound.GenWithStackByArgs(w.workerID)
	}

	executorNodeID := info.NodeID
	messageClient := w.manager.messageRouter.GetClient(executorNodeID)

	prefixedTopic := fmt.Sprintf("worker-message/%s/%s", w.workerID, topic)
	if messageClient == nil {
		return derror.ErrMessageClientNotFoundForWorker.GenWithStackByArgs(w.workerID)
	}
	_, err := messageClient.TrySendMessage(ctx, prefixedTopic, message)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *workerHandleImpl) Status() *WorkerStatus {
	w.manager.mu.Lock()
	defer w.manager.mu.Unlock()

	info, ok := w.manager.workerInfos[w.workerID]
	if !ok {
		// TODO think about how to handle this
		return nil
	}

	return &info.status
}

func (w *workerHandleImpl) Workload() model.RescUnit {
	w.manager.mu.Lock()
	defer w.manager.mu.Unlock()

	info, ok := w.manager.workerInfos[w.workerID]
	if !ok {
		// worker not found
		// TODO think about how to handle this
		return 0
	}

	return info.workload
}

func newWorkerManager(id MasterID, needWait bool, curEpoch epoch) *workerManager {
	return &workerManager{
		needWaitForHeartBeat: needWait,
		workerInfos:          make(map[WorkerID]*WorkerInfo),
		masterEpoch:          curEpoch,
		masterID:             id,
	}
}

func (m *workerManager) Initialized(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.needWaitForHeartBeat {
		return true, nil
	}

	if m.initStartTime.IsZero() {
		m.initStartTime = time.Now()
	}
	if time.Since(m.initStartTime) > workerTimeoutDuration+workerTimeoutGracefulDuration {
		return true, nil
	}
	return false, nil
}

func (m *workerManager) Tick(ctx context.Context, router p2p.MessageRouter) error {
	// respond to worker heartbeats
	for _, workerInfo := range m.workerInfos {
		if !workerInfo.hasPendingHeartbeat {
			continue
		}
		reply := &HeartbeatPongMessage{
			SendTime:  workerInfo.lastHeartBeatSendTime,
			ReplyTime: time.Now(),
			Epoch:     m.masterEpoch,
		}
		workerNodeID := workerInfo.NodeID
		log.L().Debug("Sending heartbeat response to worker",
			zap.String("worker-id", string(workerInfo.ID)),
			zap.String("worker-node-id", string(workerNodeID)),
			zap.Any("message", reply))

		msgClient := router.GetClient(workerNodeID)
		if msgClient == nil {
			// Retry on the next tick
			continue
		}
		_, err := msgClient.TrySendMessage(ctx, HeartbeatPongTopic(m.masterID), reply)
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("Sending heartbeat response to worker failed: message client congested",
				zap.String("worker-id", string(workerInfo.ID)),
				zap.String("worker-node-id", string(workerNodeID)),
				zap.Any("message", reply))
			// Retry on the next tick
			continue
		}
		workerInfo.hasPendingHeartbeat = false
	}
	return nil
}

func (m *workerManager) HandleHeartBeat(msg *HeartbeatPingMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.L().Debug("received heartbeat", zap.Any("msg", msg))
	workerInfo, ok := m.workerInfos[msg.FromWorkerID]
	if !ok {
		log.L().Info("discarding heartbeat for non-existing worker",
			zap.Any("msg", msg))
		return
	}
	workerInfo.lastHeartBeatReceiveTime = time.Now()
	workerInfo.lastHeartBeatSendTime = msg.SendTime
	workerInfo.hasPendingHeartbeat = true
}

func (m *workerManager) UpdateWorkload(msg *WorkloadReportMessage) {
	info, ok := m.getWorkerInfo(msg.WorkerID)
	if !ok {
		log.L().Info("received workload update for non-existing worker",
			zap.String("master-id", string(m.masterID)),
			zap.Any("msg", msg))
		return
	}
	info.workload = msg.Workload
	log.L().Debug("workload updated",
		zap.String("master-id", string(m.masterID)),
		zap.Any("msg", msg))
}

func (m *workerManager) UpdateStatus(msg *StatusUpdateMessage) {
	info, ok := m.getWorkerInfo(msg.WorkerID)
	if !ok {
		log.L().Info("received status update for non-existing worker",
			zap.String("master-id", string(m.masterID)),
			zap.Any("msg", msg))
		return
	}
	info.status = msg.Status
	log.L().Debug("worker status updated",
		zap.String("master-id", string(m.masterID)),
		zap.Any("msg", msg))
}

func (m *workerManager) getWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.workerInfos[id]
	if !ok {
		return nil, false
	}
	return value, true
}

func (m *workerManager) putWorkerInfo(info *WorkerInfo) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := info.ID
	_, exists := m.workerInfos[id]
	return !exists
}

func (m *workerManager) removeWorkerInfo(id WorkerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.workerInfos[id]
	delete(m.workerInfos, id)
	return exists
}

func (m *workerManager) onWorkerCreated(id WorkerID, exeuctorNodeID p2p.NodeID) {
	m.putWorkerInfo(&WorkerInfo{
		ID:                       id,
		NodeID:                   exeuctorNodeID,
		lastHeartBeatReceiveTime: time.Now(),
		status: WorkerStatus{
			Code: WorkerStatusInit,
		},
		// TODO fix workload
		workload: 10, // 10 is the initial workload for now.
	})
}

func (m *workerManager) getWorkerHandle(id WorkerID) WorkerHandle {
	return &workerHandleImpl{
		manager:  m,
		workerID: id,
	}
}
