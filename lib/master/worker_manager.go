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
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
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
	onWorkerOfflined      Callback
	onWorkerStatusUpdated Callback

	eventQueue chan *masterEvent
	closeCh    chan struct{}
	errCh      chan error

	clock clock.Clock

	timeouts config.TimeoutConfig

	wg sync.WaitGroup
}

type workerManagerState int32

const (
	workerManagerReady = workerManagerState(iota + 1)
	workerManagerWaitingHeartbeat
)

type Callback = func(ctx context.Context, handle WorkerHandle)

func NewWorkerManager(
	masterID libModel.MasterID,
	epoch libModel.Epoch,
	meta metaclient.KVClient,
	messageSender p2p.MessageSender,
	onWorkerOnline Callback,
	onWorkerOffline Callback,
	onWorkerStatusUpdated Callback,
	isInit bool,
	timeoutConfig config.TimeoutConfig,
) *WorkerManager {
	state := workerManagerReady
	if !isInit {
		state = workerManagerWaitingHeartbeat
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

		eventQueue: make(chan *masterEvent, 1024),
		closeCh:    make(chan struct{}),
		errCh:      make(chan error, 1),

		clock:    clock.New(),
		timeouts: timeoutConfig,
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		if err := ret.runBackgroundChecker(); err != nil {
			select {
			case ret.errCh <- err:
				log.L().Warn("runBackgroundChecker encountered error",
					zap.String("master-id", masterID),
					zap.Error(err))
			default:
				log.L().Warn("runBackgroundChecker error dropped",
					zap.String("master-id", masterID),
					zap.Error(err))
			}
		}
	}()

	return ret
}

func (m *WorkerManager) Close() {
	close(m.closeCh)
}

// InitAfterRecover should be called after the master has failed over.
// This method will block until a timeout period for heartbeats has passed.
func (m *WorkerManager) InitAfterRecover(ctx context.Context) error {
	timeoutInterval := m.timeouts.WorkerTimeoutDuration + m.timeouts.WorkerTimeoutGracefulDuration
	timer := time.NewTimer(timeoutInterval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-timer.C:
		// Wait for the worker timeout to expire, so
		// that we maintain all the workers that are alive.
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	allPersistedWorkers, err := m.workerMetaClient.LoadAllWorkers(ctx)
	if err != nil {
		return err
	}

	for workerID, status := range allPersistedWorkers {
		if entry, exists := m.workerEntries[workerID]; !exists {
			// For those workers we have lost contact with, we insert
			// a tombstone.
			m.workerEntries[workerID] = newTombstoneWorkerEntry(workerID, status)
		} else {
			// Put the current persisted status in the workerEntry.
			entry.InitStatus(status)
		}
	}

	m.state = workerManagerReady
	return nil
}

func (m *WorkerManager) HandleHeartbeat(msg *libModel.HeartbeatPingMessage, fromNode p2p.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.checkMasterEpochMatch(msg.Epoch) {
		return nil
	}

	entry, exists := m.workerEntries[msg.FromWorkerID]
	if !exists {
		if m.state != workerManagerWaitingHeartbeat {
			log.L().Info("Message from stale worker dropped",
				zap.String("master-id", m.masterID),
				zap.Any("message", msg),
				zap.String("from-node", fromNode))
			return nil
		}

		// We are expecting heartbeats now
		entry = newWorkerEntry(
			msg.FromWorkerID,
			model.ExecutorID(fromNode),
			m.nextExpireTime(),
			false,
			nil)
		m.workerEntries[msg.FromWorkerID] = entry

		log.L().Info("Worker discovered", zap.String("master-id", m.masterID),
			zap.Any("worker-entry", entry))

		entry.heartbeatCount.Store(1)
		// We do not call onWorkerOnlined because the master is still waiting to be contacted.
		return nil
	}

	entry.ExpireAt = m.nextExpireTime()

	newHeartbeatCount := entry.heartbeatCount.Add(1)
	if newHeartbeatCount == 1 {
		err := m.enqueueEvent(&masterEvent{
			Tp: workerOnlineEvent,
			Handle: &RunningWorkerHandle{
				workerID:   msg.FromWorkerID,
				executorID: model.ExecutorID(fromNode),
				manager:    m,
			},
		})
		if err != nil {
			return nil
		}
	}
	return nil
}

// Tick should be called by the BaseMaster so that the callbacks can be
// run in the main goroutine.
func (m *WorkerManager) Tick(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

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
			m.onWorkerOnlined(ctx, event.Handle)
		case workerOfflineEvent:
			m.onWorkerOfflined(ctx, event.Handle)
		case workerStatusUpdatedEvent:
			m.onWorkerStatusUpdated(ctx, event.Handle)
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
		false,
		&libModel.WorkerStatus{
			Code: libModel.WorkerStatusCreated,
		})
}

// OnCreatingWorkerFailed is called if we know for sure that the worker will never be created.
// This method undoes whatever OnCreatingWorker does.
func (m *WorkerManager) OnCreatingWorkerFailed(workerID libModel.WorkerID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.workerEntries, workerID)
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
			ret[workerID] = &TombstoneHandle{
				workerID: workerID,
				manager:  m,
			}
			continue
		}

		ret[workerID] = &RunningWorkerHandle{
			workerID:   workerID,
			executorID: entry.ExecutorID,
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
			if entry.ExpireAt.After(m.clock.Now()) {
				// Not timed out

				if reader := entry.StatusReader(); reader != nil {
					if _, ok := reader.Receive(); ok {
						err := m.enqueueEvent(&masterEvent{
							Tp: workerStatusUpdatedEvent,
							Handle: &RunningWorkerHandle{
								workerID:   workerID,
								executorID: entry.ExecutorID,
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
			err := m.enqueueEvent(&masterEvent{
				Tp: workerOfflineEvent,
				Handle: &TombstoneHandle{
					workerID: workerID,
					manager:  m,
				},
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
