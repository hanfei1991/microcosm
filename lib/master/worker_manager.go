package master

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/lib/config"
	"github.com/hanfei1991/microcosm/lib/metadata"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/errctx"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	dorm "github.com/hanfei1991/microcosm/pkg/meta/orm"
	libModel "github.com/hanfei1991/microcosm/pkg/meta/orm/model"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	Callback          = func(ctx context.Context, handle WorkerHandle) error
	CallbackWithError = func(ctx context.Context, handle WorkerHandle, err error) error
)

type WorkerManager struct {
	mu            sync.Mutex
	workerEntries map[libModel.WorkerID]*workerEntry
	state         workerManagerState

	workerMetaClient *metadata.WorkerMetadataClient
	messageSender    p2p.MessageSender

	masterID libModel.MasterID
	epoch    libModel.Epoch

	onWorkerOnlined       Callback
	onWorkerOfflined      CallbackWithError
	onWorkerStatusUpdated Callback
	onWorkerDispatched    CallbackWithError

	eventQueue chan *masterEvent
	closeCh    chan struct{}
	errCenter  *errctx.ErrCenter
	// allWorkersReady is **closed** when a heartbeat has been received
	// from all workers recorded in meta.
	allWorkersReady chan struct{}

	clock clock.Clock

	timeouts config.TimeoutConfig

	wg sync.WaitGroup
}

type workerManagerState int32

const (
	workerManagerReady = workerManagerState(iota + 1)
	workerManagerLoadingMeta
	workerManagerWaitingHeartbeat
)

func NewWorkerManager(
	masterID libModel.MasterID,
	epoch libModel.Epoch,
	meta *dorm.MetaOpsClient,
	messageSender p2p.MessageSender,
	onWorkerOnline Callback,
	onWorkerOffline CallbackWithError,
	onWorkerStatusUpdated Callback,
	onWorkerDispatched CallbackWithError,
	isInit bool,
	timeoutConfig config.TimeoutConfig,
	clock clock.Clock,
) *WorkerManager {
	state := workerManagerReady
	if !isInit {
		state = workerManagerLoadingMeta
	}

	ret := &WorkerManager{
		workerEntries: make(map[libModel.WorkerID]*workerEntry),
		state:         state,

		workerMetaClient: metadata.NewWorkerMetadataClient(masterID, meta),
		messageSender:    messageSender,

		masterID: masterID,
		epoch:    epoch,

		onWorkerOnlined:       onWorkerOnline,
		onWorkerOfflined:      onWorkerOffline,
		onWorkerStatusUpdated: onWorkerStatusUpdated,
		onWorkerDispatched:    onWorkerDispatched,

		eventQueue:      make(chan *masterEvent, 1024),
		closeCh:         make(chan struct{}),
		errCenter:       errctx.NewErrCenter(),
		allWorkersReady: make(chan struct{}),

		clock:    clock,
		timeouts: timeoutConfig,
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		if err := ret.runBackgroundChecker(); err != nil {
			ret.errCenter.OnError(err)
		}
	}()

	return ret
}

func (m *WorkerManager) Close() {
	close(m.closeCh)
	m.wg.Wait()
}

// InitAfterRecover should be called after the master has failed over.
// This method will block until a timeout period for heartbeats has passed.
func (m *WorkerManager) InitAfterRecover(ctx context.Context) (retErr error) {
	defer func() {
		if retErr != nil {
			m.errCenter.OnError(retErr)
		}
	}()

	ctx = m.errCenter.WithCancelOnFirstError(ctx)

	m.mu.Lock()
	if m.state != workerManagerLoadingMeta {
		// InitAfterRecover should only be called if
		// NewWorkerManager has been called with isInit as false.
		log.L().Panic("Unreachable", zap.String("master-id", m.masterID))
	}

	// Unlock here because loading meta involves I/O, which can be long.
	m.mu.Unlock()

	allPersistedWorkers, err := m.workerMetaClient.LoadAllWorkers(ctx)
	if err != nil {
		return err
	}

	if len(allPersistedWorkers) == 0 {
		// Fast path when there is no worker.
		m.mu.Lock()
		m.state = workerManagerReady
		m.mu.Unlock()
		return nil
	}

	m.mu.Lock()
	for workerID, status := range allPersistedWorkers {
		entry := newWaitingWorkerEntry(workerID, status)
		m.workerEntries[workerID] = entry
	}
	m.state = workerManagerWaitingHeartbeat
	m.mu.Unlock()

	timeoutInterval := m.timeouts.WorkerTimeoutDuration + m.timeouts.WorkerTimeoutGracefulDuration

	timer := m.clock.Timer(timeoutInterval)
	defer timer.Stop()

	startTime := m.clock.Now()
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-m.allWorkersReady:
		log.L().Info("All workers have sent heartbeats after master failover. Resuming right now.",
			zap.Duration("duration", m.clock.Since(startTime)))
	case <-timer.C:
		// Wait for the worker timeout to expire
		m.mu.Lock()
		for _, entry := range m.workerEntries {
			if entry.State() == workerEntryWait {
				entry.MarkAsTombstone()
			}
		}
		m.mu.Unlock()
	}

	m.state = workerManagerReady
	return nil
}

func (m *WorkerManager) HandleHeartbeat(msg *libModel.HeartbeatPingMessage, fromNode p2p.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state == workerManagerLoadingMeta {
		return
	}

	if !m.checkMasterEpochMatch(msg.Epoch) {
		return
	}

	entry, exists := m.workerEntries[msg.FromWorkerID]
	if !exists {
		log.L().Info("Message from stale worker dropped",
			zap.String("master-id", m.masterID),
			zap.Any("message", msg),
			zap.String("from-node", fromNode))
		return
	}

	entry.SetExpireTime(m.nextExpireTime())

	if m.state == workerManagerWaitingHeartbeat {
		if entry.State() != workerEntryWait {
			log.L().Panic("Unexpected worker entry state",
				zap.Any("entry", entry))
		}

		log.L().Info("Worker discovered", zap.String("master-id", m.masterID),
			zap.Any("worker-entry", entry))
		entry.MarkAsOnline(model.ExecutorID(fromNode), m.nextExpireTime())

		allReady := true
		for _, e := range m.workerEntries {
			if e.State() == workerEntryWait {
				allReady = false
				break
			}
		}
		if allReady {
			close(m.allWorkersReady)
			log.L().Info("All workers have sent heartbeats, sending signal to resume the master",
				zap.String("master-id", m.masterID))
		}
	} else {
		if entry.State() != workerEntryCreated {
			return
		}

		entry.MarkAsOnline(model.ExecutorID(fromNode), m.nextExpireTime())

		err := m.enqueueEvent(&masterEvent{
			Tp:       workerOnlineEvent,
			WorkerID: msg.FromWorkerID,
			Handle: &runningHandleImpl{
				workerID:   msg.FromWorkerID,
				executorID: model.ExecutorID(fromNode),
				manager:    m,
			},
		})
		if err != nil {
			m.errCenter.OnError(err)
		}
	}
}

// Tick should be called by the BaseMaster so that the callbacks can be
// run in the main goroutine.
func (m *WorkerManager) Tick(ctx context.Context) error {
	if err := m.errCenter.CheckError(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ctx = m.errCenter.WithCancelOnFirstError(ctx)

	for {
		var event *masterEvent
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case event = <-m.eventQueue:
		default:
			return nil
		}

		switch event.Tp {
		case workerOnlineEvent:
			if err := m.onWorkerOnlined(ctx, event.Handle); err != nil {
				return err
			}
		case workerOfflineEvent:
			if err := m.onWorkerOfflined(ctx, event.Handle, event.Err); err != nil {
				return err
			}
		case workerStatusUpdatedEvent:
			if err := m.onWorkerStatusUpdated(ctx, event.Handle); err != nil {
				return err
			}
		case workerDispatched:
			if err := m.onWorkerDispatched(ctx, event.Handle, event.Err); err != nil {
				return err
			}
		}

		if event.beforeHook != nil {
			event.beforeHook()
		}
	}
}

// OnCreatingWorker is called by the BaseMaster BEFORE the RPC call for creating a worker.
func (m *WorkerManager) OnCreatingWorker(workerID libModel.WorkerID, executorID model.ExecutorID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.workerEntries[workerID]; exists {
		log.L().Panic("worker already exists", zap.String("worker-id", workerID))
	}

	m.workerEntries[workerID] = newWorkerEntry(
		workerID,
		executorID,
		m.nextExpireTime(),
		workerEntryCreated,
		&libModel.WorkerStatus{
			Code: libModel.WorkerStatusCreated,
		})
}

// OnCreatingWorkerFinished is called if we know for sure that the worker will never be created.
// This method undoes whatever OnCreatingWorker does.
func (m *WorkerManager) OnCreatingWorkerFinished(workerID libModel.WorkerID, errIn error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var event *masterEvent
	if errIn != nil {
		event = &masterEvent{
			Tp:       workerDispatched,
			WorkerID: workerID,
			Handle: &tombstoneHandleImpl{
				workerID: workerID,
				manager:  m,
			},
			Err: errIn,
		}
		delete(m.workerEntries, workerID)
	} else {
		entry, exists := m.workerEntries[workerID]
		if !exists {
			log.L().Panic("unexpected call of OnCreatingWorkerFinished",
				zap.String("worker-id", workerID))
		}
		event = &masterEvent{
			Tp:       workerDispatched,
			WorkerID: workerID,
			Handle: &runningHandleImpl{
				workerID:   workerID,
				executorID: entry.executorID,
				manager:    m,
			},
			Err: errIn,
		}
	}
	err := m.enqueueEvent(event)
	if err != nil {
		log.L().Warn("workerDispatchFailed event is dropped",
			zap.Error(errIn))
	}
}

// OnWorkerStatusUpdateMessage should be called in the message handler for WorkerStatusMessage.
func (m *WorkerManager) OnWorkerStatusUpdateMessage(msg *statusutil.WorkerStatusMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.checkMasterEpochMatch(msg.MasterEpoch) {
		return
	}

	entry, exists := m.workerEntries[msg.Worker]
	if exists {
		err := entry.StatusReader().OnAsynchronousNotification(msg.Status)
		if err != nil {
			log.L().Warn("Error encountered when processing status update",
				zap.String("master-id", m.masterID),
				zap.Any("message", msg))
		}
		return
	}

	log.L().Info("WorkerStatusMessage dropped for unknown worker",
		zap.String("master-id", m.masterID),
		zap.Any("message", msg))
}

func (m *WorkerManager) GetWorkers() map[libModel.WorkerID]WorkerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()

	ret := make(map[libModel.WorkerID]WorkerHandle, len(m.workerEntries))
	for workerID, entry := range m.workerEntries {
		if entry.IsTombstone() {
			ret[workerID] = &tombstoneHandleImpl{
				workerID: workerID,
				manager:  m,
			}
			continue
		}

		ret[workerID] = &runningHandleImpl{
			workerID:   workerID,
			executorID: entry.executorID,
			manager:    m,
		}
	}
	return ret
}

func (m *WorkerManager) IsInitialized() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state == workerManagerReady
}

func (m *WorkerManager) runBackgroundChecker() error {
	ticker := time.NewTicker(m.timeouts.MasterHeartbeatCheckLoopInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.closeCh:
			log.L().Info("timeout checker exited", zap.String("master-id", m.masterID))
			return nil
		case <-ticker.C:
		}

		m.mu.Lock()
		for workerID, entry := range m.workerEntries {
			entry := entry
			state := entry.State()
			if state == workerEntryOffline || state == workerEntryTombstone {
				// Prevent repeated delivery of the workerOffline event.
				continue
			}

			if entry.ExpireTime().After(m.clock.Now()) {
				// Not timed out
				if reader := entry.StatusReader(); reader != nil {
					if _, ok := reader.Receive(); ok {
						err := m.enqueueEvent(&masterEvent{
							Tp:       workerStatusUpdatedEvent,
							WorkerID: workerID,
							Handle: &runningHandleImpl{
								workerID:   workerID,
								executorID: entry.executorID,
								manager:    m,
							},
						})
						if err != nil {
							m.mu.Unlock()
							return err
						}
					}
				}

				continue
			}

			// The worker has timed out.
			entry.MarkAsOffline()

			var offlineError error
			if reader := entry.StatusReader(); reader != nil {
				switch reader.Status().Code {
				case libModel.WorkerStatusFinished:
					offlineError = derror.ErrWorkerFinish.FastGenByArgs()
				case libModel.WorkerStatusStopped:
					offlineError = derror.ErrWorkerStop.FastGenByArgs()
				default:
					offlineError = derror.ErrWorkerOffline.FastGenByArgs(workerID)
				}
			}

			err := m.enqueueEvent(&masterEvent{
				Tp:       workerOfflineEvent,
				WorkerID: workerID,
				Handle: &tombstoneHandleImpl{
					workerID: workerID,
					manager:  m,
				},
				Err: offlineError,
				beforeHook: func() {
					entry.MarkAsTombstone()
				},
			})
			if err != nil {
				m.mu.Unlock()
				return err
			}
		}
		m.mu.Unlock()
	}
}

func (m *WorkerManager) nextExpireTime() time.Time {
	timeoutInterval := m.timeouts.WorkerTimeoutDuration + m.timeouts.WorkerTimeoutGracefulDuration
	return m.clock.Now().Add(timeoutInterval)
}

func (m *WorkerManager) checkMasterEpochMatch(msgEpoch libModel.Epoch) (ok bool) {
	if msgEpoch > m.epoch {
		// If there is a worker reporting to a master with a larger epoch, then
		// we shouldn't be running.
		// TODO We need to do some chaos testing to determining whether and how to
		// handle this situation.
		log.L().Panic("We are a stale master still running",
			zap.String("master-id", m.masterID),
			zap.Int64("msg-epoch", msgEpoch),
			zap.Int64("own-epoch", m.epoch))
	}

	if msgEpoch < m.epoch {
		log.L().Info("Message from smaller epoch dropped",
			zap.String("master-id", m.masterID),
			zap.Int64("msg-epoch", msgEpoch),
			zap.Int64("own-epoch", m.epoch))
		return false
	}
	return true
}

func (m *WorkerManager) enqueueEvent(event *masterEvent) error {
	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		return derror.ErrMasterTooManyPendingEvents.GenWithStackByArgs()
	case m.eventQueue <- event:
	}

	return nil
}
