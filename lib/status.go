package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type StatusSender struct {
	workerMetaClient *WorkerMetadataClient
	messageSender    p2p.MessageSender
	masterClient     *masterClient

	lastStatusMu sync.Mutex
	lastStatus   *WorkerStatus

	pool workerpool.AsyncPool

	clock clock.Clock

	errCh chan error
}

func NewStatusSender(
	masterClient *masterClient,
	workerMetaClient *WorkerMetadataClient,
	messageSender p2p.MessageSender,
	pool workerpool.AsyncPool,
	clock clock.Clock,
) *StatusSender {
	return &StatusSender{
		workerMetaClient: workerMetaClient,
		messageSender:    messageSender,
		masterClient:     masterClient,
		pool:             pool,
		clock:            clock,
		errCh:            make(chan error, 1),
	}
}

func (s *StatusSender) Tick(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-s.errCh:
		return errors.Trace(err)
	default:
	}

	s.lastStatusMu.Lock()
	lastStatus := s.lastStatus
	if lastStatus != nil {
		s.lastStatus = nil
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

func (s *StatusSender) SendStatus(ctx context.Context, status WorkerStatus) error {
	if err := status.marshalExt(); err != nil {
		return errors.Trace(err)
	}

	err := s.pool.Go(ctx, func() {
		if err := s.workerMetaClient.Store(ctx, &status); err != nil {
			s.OnError(err)
		}

		ok, err := s.messageSender.SendToNode(
			ctx,
			s.masterClient.MasterID(),
			workerStatusUpdatedTopic(s.masterClient.MasterID(), s.masterClient.workerID),
			status)
		if err != nil {
			s.OnError(err)
		}

		if !ok {
			// handle retry
			s.lastStatusMu.Lock()
			s.lastStatus = &status
			s.lastStatusMu.Unlock()
		}
	})
	return errors.Trace(err)
}

func (s *StatusSender) OnError(err error) {
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

type StatusReceiver struct {
	workerMetaClient      *WorkerMetadataClient
	messageHandlerManager p2p.MessageHandlerManager

	extTpi interface{}

	statusMu    sync.RWMutex
	statusCache WorkerStatus

	hasPendingNotification atomic.Bool
	lastStatusUpdated      atomic.Time

	clock clock.Clock
}

func NewStatusReceiver(
	workerMetaClient *WorkerMetadataClient,
	messageHandlerManager p2p.MessageHandlerManager,
	extTpi interface{},
	clock clock.Clock,
) *StatusReceiver {
	return &StatusReceiver{
		workerMetaClient:      workerMetaClient,
		messageHandlerManager: messageHandlerManager,
		extTpi:                extTpi,
		clock:                 clock,
	}
}

func (r *StatusReceiver) Init(ctx context.Context) error {
	initStatus, err := r.workerMetaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	r.statusMu.Lock()
	defer r.statusMu.Unlock()

	if err := initStatus.fillExt(r.extTpi); err != nil {
		return errors.Trace(err)
	}

	r.statusCache = *initStatus
	r.lastStatusUpdated.Store(r.clock.Now())

	return nil
}

func (r *StatusReceiver) Status() WorkerStatus {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	return r.statusCache
}

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

	if err := status.fillExt(r.extTpi); err != nil {
		return errors.Trace(err)
	}

	r.statusCache = *status
	r.lastStatusUpdated.Store(r.clock.Now())

	return nil
}
