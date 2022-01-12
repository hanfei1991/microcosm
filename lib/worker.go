package lib

import (
	"context"
	"sync"
	"time"

	"github.com/gavv/monotime"
	"github.com/hanfei1991/microcosm/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type Worker interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() WorkerID
	Close()
}

type WorkerImpl interface {
	Worker

	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval.
	Tick(ctx context.Context) error

	// Status returns a short worker status to be periodically sent to the master.
	Status() (WorkerStatus, error)

	// Workload returns the current workload of the worker.
	Workload() (model.RescUnit, error)

	// OnMasterFailover is called when the master is failed over.
	OnMasterFailover(reason MasterFailoverReason) error

	// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
	CloseImpl()
}

type BaseWorker struct {
	impl WorkerImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageRouter         p2p.MessageRouter
	metaKVClient          metadata.MetaKV

	master *masterManager

	id WorkerID

	wg    sync.WaitGroup
	errCh chan error
}

func NewBaseWorker(
	ctx context.Context,
	impl WorkerImpl,
	messageHandlerManager p2p.MessageHandlerManager,
	messageRouter p2p.MessageRouter,
	metaKVClient metadata.MetaKV,
	workerID WorkerID,
	masterID MasterID,
) *BaseWorker {
	masterManager := newMasterManager(masterID, workerID, messageRouter)
	return &BaseWorker{
		impl:                  impl,
		messageHandlerManager: messageHandlerManager,
		messageRouter:         messageRouter,
		metaKVClient:          metaKVClient,
		master:                masterManager,
		id:                    workerID,
		errCh:                 make(chan error, 1),
	}
}

func (w *BaseWorker) Init(ctx context.Context) error {
	if err := w.initMessageHandlers(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := w.impl.InitImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	w.startBackgroundTasks(ctx)
	return nil
}

func (w *BaseWorker) Poll(ctx context.Context) error {
	if err := w.messageHandlerManager.CheckError(ctx); err != nil {
		return errors.Trace(err)
	}

	select {
	case err := <-w.errCh:
		if err != nil {
			return errors.Trace(err)
		}
	default:
	}

	if hasTimedOut := w.master.CheckMasterTimeout(); hasTimedOut {
		return derror.ErrWorkerSuicide.GenWithStackByArgs()
	}

	if err := w.impl.Poll(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *BaseWorker) Close() {
	w.impl.CloseImpl()

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := w.messageHandlerManager.Clean(closeCtx); err != nil {
		log.L().Warn("cleaning message handlers failed",
			zap.Error(err))
	}
}

func (w *BaseWorker) MetaKVClient() metadata.MetaKV {
	return w.metaKVClient
}

func (w *BaseWorker) startBackgroundTasks(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.runStatusWorker(ctx); err != nil {
			w.onError(err)
		}
	}()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.runHeartbeatWorker(ctx); err != nil {
			w.onError(err)
		}
	}()
}

func (w *BaseWorker) runHeartbeatWorker(ctx context.Context) error {
	ticker := time.NewTicker(workerHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := w.master.SendHeartBeat(ctx); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *BaseWorker) runStatusWorker(ctx context.Context) error {
	ticker := time.NewTicker(workerReportStatusInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			status, err := w.impl.Status()
			if err != nil {
				return errors.Trace(err)
			}
			workload, err := w.impl.Workload()
			if err != nil {
				return errors.Trace(err)
			}
			if err := w.master.SendStatus(ctx, status, workload); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *BaseWorker) initMessageHandlers(ctx context.Context) error {
	topic := HeartbeatPongTopic(w.master.MasterID())
	ok, err := w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&HeartbeatPongMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*HeartbeatPongMessage)
			log.L().Debug("heartbeat pong received",
				zap.Any("msg", msg))
			w.master.HandleHeartbeat(msg)
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

func (w *BaseWorker) onError(err error) {
	select {
	case w.errCh <- err:
	default:
	}
}

type masterManager struct {
	mu          sync.RWMutex
	masterID    MasterID
	masterNode  p2p.NodeID
	masterEpoch Epoch

	workerID WorkerID

	messageRouter           p2p.MessageRouter
	metaKVClient            metadata.MetaKV
	lastMasterAckedPingTime monotonicTime
}

func newMasterManager(masterID MasterID, workerID WorkerID, messageRouter p2p.MessageRouter) *masterManager {
	return &masterManager{
		masterID:      masterID,
		workerID:      workerID,
		messageRouter: messageRouter,
	}
}

func (m *masterManager) refreshMasterInfo(ctx context.Context) error {
	metaClient := NewMetadataClient(m.masterID, m.metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.masterNode = masterMeta.NodeID
	m.masterEpoch = masterMeta.Epoch

	return nil
}

func (m *masterManager) MasterID() MasterID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterID
}

func (m *masterManager) MasterNodeID() p2p.NodeID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterNode
}

func (m *masterManager) HandleHeartbeat(msg *HeartbeatPongMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// TODO think about whether to distinct msg.Epoch > m.masterEpoch
	// and msg.Epoch < m.masterEpoch
	if msg.Epoch != m.masterEpoch {
		log.L().Info("epoch does not match",
			zap.Any("msg", msg),
			zap.Int64("master-epoch", m.masterEpoch))
		return
	}
	m.lastMasterAckedPingTime = msg.SendTime
}

func (m *masterManager) CheckMasterTimeout() (ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return monotime.Since(m.lastMasterAckedPingTime) <= workerTimeoutDuration
}

func (m *masterManager) SendHeartBeat(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	client := m.messageRouter.GetClient(m.masterNode)
	if client == nil {
		log.L().Warn("master node not found", zap.String("node-id", m.masterNode))
		// TODO think whether it is appropriate to return nil here.
		return nil
	}

	heartbeatMsg := &HeartbeatPingMessage{
		SendTime:     monotime.Now(),
		FromWorkerID: m.workerID,
		Epoch:        m.masterEpoch,
	}
	_, err := client.TrySendMessage(ctx, HeartbeatPingTopic(m.masterID), heartbeatMsg)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("sending heartbeat ping encountered ErrPeerMessageSendTryAgain")
			return nil
		}
		return errors.Trace(err)
	}
	return nil
}

func (m *masterManager) SendStatus(ctx context.Context, status WorkerStatus, workload model.RescUnit) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	client := m.messageRouter.GetClient(m.masterNode)
	if client == nil {
		log.L().Warn("master node not found", zap.String("node-id", m.masterNode))
		// TODO think whether it is appropriate to return nil here.
		return nil
	}

	statusUpdateMessage := &StatusUpdateMessage{
		WorkerID: m.workerID,
		Status:   status,
	}
	_, err := client.TrySendMessage(ctx, StatusUpdateTopic(m.masterID), statusUpdateMessage)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("sending status update encountered ErrPeerMessageSendTryAgain")
			return nil
		}
		return errors.Trace(err)
	}

	workloadReportMessage := &WorkloadReportMessage{
		WorkerID: m.workerID,
		Workload: workload,
	}
	_, err = client.TrySendMessage(ctx, WorkloadReportTopic(m.masterID), workloadReportMessage)
	if err != nil {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("sending workload encountered ErrPeerMessageSendTryAgain")
			return nil
		}
		return errors.Trace(err)
	}

	return nil
}
