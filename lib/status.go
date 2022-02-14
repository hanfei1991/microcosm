package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// StatusSender is used for a worker to send its status to its master.
type StatusSender struct {
	workerMetaClient *WorkerMetadataClient
	messageSender    p2p.MessageSender
	masterClient     *masterClient

	lastStatusMu     sync.Mutex
	lastUnsentStatus *WorkerStatus

	pool workerpool.AsyncPool

	errCh chan error
}

// NewStatusSender returns a new StatusSender.
// NOTE: the pool is owned by the caller.
func NewStatusSender(
	masterClient *masterClient,
	workerMetaClient *WorkerMetadataClient,
	messageSender p2p.MessageSender,
	pool workerpool.AsyncPool,
) *StatusSender {
	return &StatusSender{
		workerMetaClient: workerMetaClient,
		messageSender:    messageSender,
		masterClient:     masterClient,
		pool:             pool,
		errCh:            make(chan error, 1),
	}
}

// Tick should be called periodically to drive the logic internal to StatusSender.
func (s *StatusSender) Tick(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-s.errCh:
		return errors.Trace(err)
	default:
	}

	s.lastStatusMu.Lock()
	lastStatus := s.lastUnsentStatus
	if lastStatus != nil {
		s.lastUnsentStatus = nil
	}
	s.lastStatusMu.Unlock()

	if lastStatus == nil {
		return nil
	}

	if err := s.SendStatus(ctx, *lastStatus); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SendStatus is used by the business logic in a worker to notify its master
// of a status change.
// This function is non-blocking and if any error occurred during or after network IO,
// the subsequent Tick will return an error.
func (s *StatusSender) SendStatus(ctx context.Context, status WorkerStatus) error {
	err := s.pool.Go(ctx, func() {
		if err := s.workerMetaClient.Store(ctx, &status); err != nil {
			s.onError(err)
		}

		ok, err := s.messageSender.SendToNode(
			ctx,
			s.masterClient.MasterNode(),
			workerStatusUpdatedTopic(s.masterClient.MasterID(), s.masterClient.workerID),
			&workerStatusUpdatedMessage{Epoch: s.masterClient.Epoch()})
		if err != nil {
			s.onError(err)
		}

		if !ok {
			// handle retry
			s.lastStatusMu.Lock()
			s.lastUnsentStatus = &status
			s.lastStatusMu.Unlock()
		}
	})
	return errors.Trace(err)
}

func (s *StatusSender) onError(err error) {
	select {
	case s.errCh <- err:
	default:
		log.L().Warn("error is dropped because errCh is full",
			zap.Error(err))
	}
}

func workerStatusUpdatedTopic(masterID MasterID, workerID WorkerID) string {
	return fmt.Sprintf("worker-status-updated-%s-%s", masterID, workerID)
}

type workerStatusUpdatedMessage struct {
	Epoch Epoch
}

// StatusReceiver is used by a master to receive the latest status update from **a** worker.
type StatusReceiver struct {
	workerMetaClient      *WorkerMetadataClient
	messageHandlerManager p2p.MessageHandlerManager

	statusMu    sync.RWMutex
	statusCache WorkerStatus

	hasPendingNotification atomic.Bool
	lastStatusUpdated      atomic.Time

	epoch Epoch

	clock clock.Clock
}

// NewStatusReceiver returns a new StatusReceiver
// NOTE: the messageHandlerManager is NOT owned by the StatusReceiver,
// and it only uses it to register a handler. It is not responsible
// for checking errors and cleaning up.
func NewStatusReceiver(
	workerMetaClient *WorkerMetadataClient,
	messageHandlerManager p2p.MessageHandlerManager,
	epoch Epoch,
	clock clock.Clock,
) *StatusReceiver {
	return &StatusReceiver{
		workerMetaClient:      workerMetaClient,
		messageHandlerManager: messageHandlerManager,
		epoch:                 epoch,
		clock:                 clock,
	}
}

// Init should be called to initialize a StatusReceiver.
// NOTE: this function can be blocked by IO to the metastore.
func (r *StatusReceiver) Init(ctx context.Context) error {
	topic := StatusUpdateTopic(r.workerMetaClient.MasterID(), r.workerMetaClient.WorkerID())
	ok, err := r.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&workerStatusUpdatedMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			log.L().Debug("Received workerStatusUpdatedMessage",
				zap.String("sender", sender),
				zap.Any("value", value))

			msg := value.(*workerStatusUpdatedMessage)
			if msg.Epoch != r.epoch {
				return nil
			}
			r.hasPendingNotification.Store(true)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handlers", zap.String("topic", topic))
	}

	initStatus, err := r.workerMetaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	r.statusMu.Lock()
	defer r.statusMu.Unlock()

	r.statusCache = *initStatus
	r.lastStatusUpdated.Store(r.clock.Now())

	return nil
}

// Status returns the latest status of the worker.
func (r *StatusReceiver) Status() WorkerStatus {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	return r.statusCache
}

// Tick should be called periodically to drive the logic internal to StatusReceiver.
func (r *StatusReceiver) Tick(ctx context.Context) error {
	// TODO make the time interval configurable
	needFetchStatus := r.hasPendingNotification.Swap(false) ||
		r.clock.Since(r.lastStatusUpdated.Load()) > time.Second*10

	if !needFetchStatus {
		return nil
	}

	r.statusMu.Lock()
	defer r.statusMu.Unlock()

	status, err := r.workerMetaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	r.statusCache = *status
	r.lastStatusUpdated.Store(r.clock.Now())

	return nil
}
