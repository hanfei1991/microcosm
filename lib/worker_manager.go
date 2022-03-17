package lib

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/dig"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// workerManager is for private use by BaseMaster.
type workerManager interface {
	IsInitialized() bool
	CheckError() error
	Tick(ctx context.Context) (offlinedWorkers []*WorkerInfo, onlinedWorkers []*WorkerInfo)
	HandleHeartbeat(msg *HeartbeatPingMessage, fromNode p2p.NodeID) error
	OnWorkerCreated(ctx context.Context, id WorkerID, exeuctorNodeID p2p.NodeID) error
	OnWorkerOffline(ctx context.Context, id WorkerID) error
	GetWorkerInfo(id WorkerID) (*WorkerInfo, bool)
	PutWorkerInfo(info *WorkerInfo) bool
	MessageSender() p2p.MessageSender
	GetWorkerHandle(id WorkerID) WorkerHandle
	GetWorkers() map[WorkerID]WorkerHandle
	GetStatus(id WorkerID) (*WorkerStatus, bool)
	CheckStatusUpdate(ctx context.Context) error
	OnWorkerStatusUpdated(msg *WorkerStatusUpdatedMessage)
}

type workerManagerFsmState = int32

const (
	workerManagerCreated = workerManagerFsmState(iota + 1)
	workerManagerLoadingAllWorkers
	workerManagerWaitingHeartbeats
	workerManagerNormal
	workerManagerFailed
)

type workerManagerImpl struct {
	mu              sync.Mutex
	initialized     bool
	initStartTime   time.Time
	workerInfos     map[WorkerID]*WorkerInfo
	tombstones      map[WorkerID]*WorkerStatus
	statusReceivers map[WorkerID]*StatusReceiver

	fsmState atomic.Int32
	errCh    chan error

	// read-only
	masterEpoch   Epoch
	masterID      MasterID
	timeoutConfig TimeoutConfig

	// to help unit testing
	clock clock.Clock

	messageSender        p2p.MessageSender
	messageHandleManager p2p.MessageHandlerManager
	metaClient           metaclient.KVClient
	pool                 workerpool.AsyncPool
}

type workerManagerParams struct {
	dig.In

	MessageSender        p2p.MessageSender
	MessageHandleManager p2p.MessageHandlerManager
	MetaClient           metaclient.KVClient
	Pool                 workerpool.AsyncPool
}

func newWorkerManager(
	ctx *dcontext.Context,
	id MasterID,
	needWait bool,
	curEpoch Epoch,
	timeoutConfig *TimeoutConfig,
) workerManager {
	var params workerManagerParams
	if err := ctx.Deps().Fill(&params); err != nil {
		log.L().Panic("failed to inject dependencies for WorkerManager",
			zap.Error(err),
			zap.String("master-id", id))
	}

	initFsmState := workerManagerCreated
	if !needWait {
		initFsmState = workerManagerNormal
	}

	return &workerManagerImpl{
		initialized:     !needWait,
		workerInfos:     make(map[WorkerID]*WorkerInfo),
		tombstones:      make(map[WorkerID]*WorkerStatus),
		statusReceivers: make(map[WorkerID]*StatusReceiver),

		fsmState: *atomic.NewInt32(initFsmState),
		errCh:    make(chan error, 1),

		masterEpoch:   curEpoch,
		masterID:      id,
		timeoutConfig: *timeoutConfig,

		clock: clock.New(),

		messageSender:        params.MessageSender,
		messageHandleManager: params.MessageHandleManager,
		metaClient:           params.MetaClient,
		pool:                 params.Pool,
	}
}

func (m *workerManagerImpl) IsInitialized() bool {
	return m.fsmState.Load() == workerManagerNormal
}

func (m *workerManagerImpl) checkGracefulPeriodFinished() {
	if st := m.fsmState.Load(); st != workerManagerWaitingHeartbeats {
		log.L().Panic("unexpected workerManager FSM state", zap.Int32("state", st))
	}

	if m.initStartTime.IsZero() {
		m.initStartTime = m.clock.Now()
	}

	thresholdDuration := m.timeoutConfig.workerTimeoutDuration + m.timeoutConfig.workerTimeoutGracefulDuration
	duration := m.clock.Since(m.initStartTime)
	if duration > thresholdDuration {
		if old := m.fsmState.Swap(workerManagerNormal); old != workerManagerWaitingHeartbeats {
			log.L().Panic("unexpected workerManager FSM state", zap.Int32("state", old))
		}
	}
}

func (m *workerManagerImpl) asyncLoadAllWorkers(ctx context.Context) error {
	if old := m.fsmState.Swap(workerManagerLoadingAllWorkers); old != workerManagerCreated {
		log.L().Panic("unexpected workerManager FSM state", zap.Int32("state", old))
	}

	err := m.pool.Go(ctx, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		workerMetaClient := NewWorkerMetadataClient(m.masterID, m.metaClient)
		workerStatuses, err := workerMetaClient.LoadAllWorkers(ctx)
		if err != nil {
			select {
			case m.errCh <- errors.Trace(err):
			default:
			}
			m.fsmState.Store(workerManagerFailed)
			return
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		for workerID, workerStatus := range workerStatuses {
			// Inserts the persisted WorkerStatus into the tombstone list.
			// If the worker is proven alive, it will be removed from the
			// tombstone list.
			m.tombstones[workerID] = workerStatus
		}
		if old := m.fsmState.Swap(workerManagerWaitingHeartbeats); old != workerManagerLoadingAllWorkers {
			log.L().Panic("unexpected workerManager FSM state", zap.Int32("state", old))
		}
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *workerManagerImpl) asyncDeleteTombstone(ctx context.Context, id WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.tombstones[id]; !exists {
		return nil
	}

	err := m.pool.Go(ctx, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		workerMetaClient := NewWorkerMetadataClient(m.masterID, m.metaClient)
		ok, err := workerMetaClient.Remove(ctx, id)
		if err != nil {
			select {
			case m.errCh <- errors.Trace(err):
			default:
			}
			m.fsmState.Store(workerManagerFailed)
			return
		}
		if !ok {
			log.L().Warn("Could not remove tombstone state for worker",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", id))
			return
		}
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *workerManagerImpl) OnWorkerStatusUpdated(msg *WorkerStatusUpdatedMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	receiver, ok := m.statusReceivers[msg.FromWorkerID]
	if !ok {
		log.L().Warn("Received worker status notification for non-existing worker",
			zap.Any("msg", msg))
		return
	}

	receiver.OnNotification(msg)
}

func (m *workerManagerImpl) CheckStatusUpdate(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, receiver := range m.statusReceivers {
		if err := receiver.Tick(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *workerManagerImpl) Tick(
	ctx context.Context,
) (offlinedWorkers []*WorkerInfo, onlinedWorkers []*WorkerInfo) {
	switch m.fsmState.Load() {
	case workerManagerCreated:
		if err := m.asyncLoadAllWorkers(ctx); err != nil {
			// TODO handle error gracefully.
			log.L().Panic("Failed to load all workers", zap.Error(err))
		}
		return
	case workerManagerLoadingAllWorkers:
		return
	case workerManagerWaitingHeartbeats:
		m.checkGracefulPeriodFinished()

		fallthrough
	case workerManagerNormal:
	default:
		log.L().Panic("unreachable")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// TODO gracefully handle all errors that could occur in this function
	// respond to worker heartbeats
	for workerID, workerInfo := range m.workerInfos {
		// `justOnlined` indicates that the online event has not been notified,
		// and `hasPendingHeartbeat` indicates that we have received a heartbeat and
		// has not sent the Pong yet.ctx context.Context,
		if workerInfo.justOnlined && workerInfo.hasPendingHeartbeat && workerInfo.statusInitialized.Load() {
			workerInfo.justOnlined = false
			onlinedWorkers = append(onlinedWorkers, workerInfo)
		}

		if workerInfo.hasTimedOut(m.clock, &m.timeoutConfig) {
			offlinedWorkers = append(offlinedWorkers, workerInfo)
			status := m.statusReceivers[workerID].Status()
			m.tombstones[workerID] = &status
			delete(m.workerInfos, workerID)
		}

		if !workerInfo.hasPendingHeartbeat {
			// No heartbeat to respond to.
			continue
		}
		reply := &HeartbeatPongMessage{
			SendTime:   workerInfo.lastHeartbeatSendTime,
			ReplyTime:  m.clock.Now(),
			ToWorkerID: workerID,
			Epoch:      m.masterEpoch,
		}
		workerNodeID := workerInfo.NodeID
		log.L().Debug("Sending heartbeat response to worker",
			zap.Any("worker-id", workerInfo.ID),
			zap.String("worker-node-id", workerNodeID),
			zap.Any("message", reply))

		ok, err := m.messageSender.SendToNode(ctx, workerNodeID, HeartbeatPongTopic(m.masterID, workerID), reply)
		if err != nil {
			log.L().Error("Failed to send heartbeat", zap.Error(err))
		}
		if !ok {
			log.L().Info("Sending heartbeat would block, will try again",
				zap.Any("message", reply))
			continue
		}
		// We have sent the Pong, mark it as such.
		workerInfo.hasPendingHeartbeat = false
	}
	return
}

func (m *workerManagerImpl) HandleHeartbeat(msg *HeartbeatPingMessage, fromNode p2p.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.L().Debug("received heartbeat", zap.Any("msg", msg))
	workerInfo, ok := m.workerInfos[msg.FromWorkerID]
	if !ok {
		if m.initialized {
			log.L().Info("discarding heartbeat from non-existing worker, probably zombie?",
				zap.Any("msg", msg),
				zap.String("node-id", fromNode))
			return nil
		}
		// We are still waiting to take over workers created in previous epochs, so
		// it is possible to encounter a worker that is not created by us. In this case,
		// we need to add the worker.
		if err := m.addWorker(msg.FromWorkerID, fromNode); err != nil {
			return errors.Trace(err)
		}
		workerInfo, ok = m.getWorkerInfo(msg.FromWorkerID)
		if !ok {
			log.L().Panic("unreachable",
				zap.Any("msg", msg),
				zap.String("node-id", fromNode))
		}
	}
	workerInfo.lastHeartbeatReceiveTime = m.clock.Now()
	workerInfo.lastHeartbeatSendTime = msg.SendTime
	workerInfo.hasPendingHeartbeat = true
	return nil
}

func (m *workerManagerImpl) GetStatus(id WorkerID) (*WorkerStatus, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	receiver, exists := m.statusReceivers[id]
	if !exists {
		return nil, false
	}

	// TODO evaluate whether we need an object pool to mitigate allocation burden.
	ret := receiver.Status()
	return &ret, true
}

func (m *workerManagerImpl) GetWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.getWorkerInfo(id)
}

func (m *workerManagerImpl) getWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	value, ok := m.workerInfos[id]
	if !ok {
		return nil, false
	}
	return value, true
}

func (m *workerManagerImpl) PutWorkerInfo(info *WorkerInfo) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.putWorkerInfo(info)
}

func (m *workerManagerImpl) putWorkerInfo(info *WorkerInfo) bool {
	id := info.ID
	_, exists := m.workerInfos[id]
	m.workerInfos[id] = info
	return !exists
}

func (m *workerManagerImpl) addWorker(id WorkerID, executorNodeID p2p.NodeID) error {
	if _, exists := m.workerInfos[id]; exists {
		// TODO determine whether it is appropriate to panic here.
		log.L().Panic("duplicate worker ID",
			zap.String("worker-id", id))
	}

	if _, exists := m.tombstones[id]; exists {
		if m.fsmState.Load() != workerManagerWaitingHeartbeats {
			// TODO: confirm whether this check is needed
			// when the workerID doesn't change, such as failover of a job master,
			// the check will be true here
			log.L().Warn("Discovered a worker whose status is not persisted",
				zap.String("worker-id", id), zap.String("executor-id", executorNodeID),
				zap.Int32("fsm-state", m.fsmState.Load()),
			)
		}
		delete(m.tombstones, id)
	}

	m.workerInfos[id] = &WorkerInfo{
		ID:                       id,
		NodeID:                   executorNodeID,
		lastHeartbeatReceiveTime: m.clock.Now(),
		// TODO fix workload
		workload:    10, // 10 is the initial workload for now.
		justOnlined: true,
	}

	workerMetaClient := NewWorkerMetadataClient(m.masterID, m.metaClient)
	receiver := NewStatusReceiver(
		id,
		workerMetaClient,
		m.messageHandleManager,
		m.masterEpoch,
		m.pool,
		m.clock,
	)
	m.statusReceivers[id] = receiver

	// TODO refine AsyncPool or refactor this function to avoid
	// possible deadlocking when the pool's pending queue is full.
	//
	// TODO figure out what context to use here.
	err := m.pool.Go(context.TODO(), func() {
		if err := receiver.Init(context.TODO()); err != nil {
			// TODO handle the error
			log.L().Warn("failed to init StatusReceiver",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", id),
				zap.Error(err))
		}
		info, ok := m.GetWorkerInfo(id)
		if !ok {
			log.L().Warn("worker has been removed",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", id))
		}
		if old := info.statusInitialized.Swap(true); old {
			log.L().Panic("worker is initialized twice. Report a bug",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", id))
		}
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *workerManagerImpl) OnWorkerCreated(ctx context.Context, id WorkerID, executorID p2p.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	workerMetaClient := NewWorkerMetadataClient(m.masterID, m.metaClient)
	err := workerMetaClient.Store(ctx, id, &WorkerStatus{
		Code: WorkerStatusCreated,
	})
	if err != nil {
		return errors.Trace(err)
	}

	if err := m.addWorker(id, executorID); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *workerManagerImpl) OnWorkerOffline(ctx context.Context, id WorkerID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.statusReceivers[id]; !ok {
		log.L().Warn("worker not found in status receivers", zap.String("workerID", id))
	}
	delete(m.statusReceivers, id)
	return nil
}

func (m *workerManagerImpl) MessageSender() p2p.MessageSender {
	return m.messageSender
}

func (m *workerManagerImpl) GetWorkerHandle(id WorkerID) WorkerHandle {
	if _, exists := m.workerInfos[id]; exists {
		return &workerHandleImpl{
			manager: m,
			id:      id,
		}
	} else if status, exists := m.tombstones[id]; exists {
		return NewTombstoneWorkerHandle(id, *status, m)
	}
	return nil
}

// GetWorkers returns a map from WorkerID to WorkerHandle for all known workers.
// NOTE this is a preliminary implementation and is likely to have performance problem
// if called too frequently.
// TODO cache the returned map, or rethink how to manage the list.
func (m *workerManagerImpl) GetWorkers() map[WorkerID]WorkerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := make(map[WorkerID]WorkerHandle)
	for workerID := range m.workerInfos {
		ret[workerID] = m.GetWorkerHandle(workerID)
	}

	for workerID, status := range m.tombstones {
		ret[workerID] = NewTombstoneWorkerHandle(workerID, *status, m)
	}
	return ret
}

func (m *workerManagerImpl) CheckError() error {
	select {
	case err := <-m.errCh:
		return err
	default:
		return nil
	}
}

type WorkerInfo struct {
	ID     WorkerID
	NodeID p2p.NodeID

	// fields for internal use by the Master.
	lastHeartbeatReceiveTime time.Time
	lastHeartbeatSendTime    clock.MonotonicTime
	hasPendingHeartbeat      bool
	justOnlined              bool

	// marks whether the status has been asynchronously
	// loaded from the metastore.
	statusInitialized atomic.Bool

	workload model.RescUnit
}

func (w *WorkerInfo) hasTimedOut(clock clock.Clock, config *TimeoutConfig) bool {
	duration := clock.Since(w.lastHeartbeatReceiveTime)
	if duration > config.workerTimeoutDuration {
		// TODO add details about the worker.
		log.L().Warn("Worker timed out", zap.Duration("duration", duration), zap.String("id", w.ID))
		return true
	}
	return false
}

type WorkerHandle interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error
	Status() *WorkerStatus
	ID() WorkerID
	IsTombStone() bool
	ToPB() (*pb.WorkerInfo, error)

	// DeleteTombStone attempts to remove the persisted status
	// if the WorkerHandler is a tombstone.
	// If the handle does not represent a deletable tombstone,
	// a call to DeleteTombStone will return false.
	DeleteTombStone(ctx context.Context) (bool, error)
}

type workerHandleImpl struct {
	manager *workerManagerImpl

	// TODO think about how to handle the situation where the workerID has been removed from `manager`.
	id WorkerID
}

func (w *workerHandleImpl) ToPB() (*pb.WorkerInfo, error) {
	statusBytes, err := w.Status().Marshal()
	if err != nil {
		return nil, err
	}

	info, ok := w.manager.GetWorkerInfo(w.id)
	if !ok {
		// TODO add an appropriate error
		return nil, nil
	}

	ret := &pb.WorkerInfo{
		Id:         w.ID(),
		ExecutorId: info.NodeID,
		Status:     statusBytes,
		LastHbTime: info.lastHeartbeatReceiveTime.Unix(),
		Workload:   int64(info.workload),
	}
	return ret, nil
}

func (w *workerHandleImpl) SendMessage(
	ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool,
) (err error) {
	info, ok := w.manager.GetWorkerInfo(w.id)
	if !ok {
		return derror.ErrWorkerNotFound.GenWithStackByArgs(w.id)
	}

	executorNodeID := info.NodeID
	if nonblocking {
		_, err = w.manager.MessageSender().SendToNode(ctx, executorNodeID, topic, message)
	} else {
		err = w.manager.MessageSender().SendToNodeB(ctx, executorNodeID, topic, message)
	}
	return
}

func (w *workerHandleImpl) GetWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	return w.manager.GetWorkerInfo(id)
}

func (w *workerHandleImpl) Status() *WorkerStatus {
	// TODO come up with a better solution when the status does not exist
	status, exists := w.manager.GetStatus(w.id)
	if !exists {
		return nil
	}
	return status
}

func (w *workerHandleImpl) ID() WorkerID {
	return w.id
}

func (w *workerHandleImpl) IsTombStone() bool {
	return false
}

func (w *workerHandleImpl) DeleteTombStone(_ context.Context) (bool, error) {
	return false, nil
}

type tombstoneWorkerHandleImpl struct {
	id      WorkerID
	status  WorkerStatus
	manager workerManager
}

func NewTombstoneWorkerHandle(id WorkerID, status WorkerStatus, manager workerManager) WorkerHandle {
	return &tombstoneWorkerHandleImpl{
		id:      id,
		status:  status,
		manager: manager,
	}
}

func (h *tombstoneWorkerHandleImpl) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	return derror.ErrWorkerOffline.GenWithStackByArgs(h.id)
}

func (h *tombstoneWorkerHandleImpl) Status() *WorkerStatus {
	return &h.status
}

func (h *tombstoneWorkerHandleImpl) ToPB() (*pb.WorkerInfo, error) {
	// TODO add an appropriate error
	return nil, nil
}

func (h *tombstoneWorkerHandleImpl) Workload() model.RescUnit {
	return 0
}

func (h *tombstoneWorkerHandleImpl) ID() WorkerID {
	return h.id
}

func (h *tombstoneWorkerHandleImpl) IsTombStone() bool {
	return true
}

func (h *tombstoneWorkerHandleImpl) DeleteTombStone(ctx context.Context) (bool, error) {
	if h.manager == nil {
		return false, nil
	}
	managerImpl, ok := h.manager.(*workerManagerImpl)
	if !ok {
		return false, nil
	}

	if err := managerImpl.asyncDeleteTombstone(ctx, h.id); err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}
