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
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type WorkerManager struct {
	mu            sync.Mutex
	workerEntries map[libModel.WorkerID]*workerEntry
	state         workerManagerState

	workerMetaClient *metadata.WorkerMetadataClient

	masterID libModel.MasterID
	epoch    libModel.Epoch

	onWorkerOnlined  func(ctx context.Context, handle WorkerHandle)
	onWorkerOfflined func(ctx context.Context, handle WorkerHandle)

	clock clock.Clock

	timeouts config.TimeoutConfig
}

type workerManagerState int32

const (
	workerManagerReady = workerManagerState(iota + 1)
	workerManagerWaitingHeartbeat
)

func NewWorkerManager(meta metaclient.KVClient, masterID libModel.MasterID) *WorkerManager {
	return &WorkerManager{
		workerMetaClient: metadata.NewWorkerMetadataClient(masterID, meta),
	}
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
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	allPersistedWorkers, err := m.workerMetaClient.LoadAllWorkers(ctx)
	if err != nil {
		return err
	}

	for workerID := range allPersistedWorkers {
		if _, exists := m.workerEntries[workerID]; !exists {
			// Handle tombstone
		}
	}

	m.state = workerManagerReady
	return nil
}

func (m *WorkerManager) HandleHeartbeat(msg *libModel.HeartbeatPingMessage, fromNode p2p.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if msg.Epoch > m.epoch {
		// If there is a worker reporting to a master with a larger epoch, then
		// we shouldn't be running.
		// TODO We need to do some chaos testing to determining whether and how to
		// handle this situation.
		log.L().Panic("We are a stale master still running",
			zap.String("master-id", m.masterID),
			zap.Int64("own-epoch", m.epoch),
			zap.Any("message", msg),
			zap.String("from-node", fromNode))
	}

	if msg.Epoch < m.epoch {
		log.L().Info("Message from smaller epoch dropped",
			zap.String("master-id", m.masterID),
			zap.Int64("own-epoch", m.epoch),
			zap.Any("message", msg),
			zap.String("from-node", fromNode))
		return nil
	}

	timeoutInterval := m.timeouts.WorkerTimeoutDuration + m.timeouts.WorkerTimeoutGracefulDuration
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
		entry = newWorkerEntry(msg.FromWorkerID, model.ExecutorID(fromNode), m.clock.Now().Add(timeoutInterval))
		m.workerEntries[msg.FromWorkerID] = entry

		log.L().Info("Worker discovered", zap.String("master-id", m.masterID),
			zap.Any("worker-entry", entry))

		return nil
	}

	entry.ExpireAt = m.clock.Now().Add(timeoutInterval)
	return nil
}

func (m *WorkerManager) handleTombstone(workerID libModel.WorkerID, status *libModel.WorkerStatus) {

}
