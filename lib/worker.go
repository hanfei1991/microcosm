package lib

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
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
	WorkerID() WorkerID
	Workload() model.RescUnit
	Close(ctx context.Context) error
}

type WorkerImpl interface {
	// InitImpl provides customized logic for the business logic to initialize.
	InitImpl(ctx context.Context) error

	// Tick is called on a fixed interval.
	Tick(ctx context.Context) error

	// Status returns a short worker status to be periodically sent to the master.
	Status() (WorkerStatus, error)

	// Workload returns the current workload of the worker.
	Workload() model.RescUnit

	// OnMasterFailover is called when the master is failed over.
	OnMasterFailover(reason MasterFailoverReason) error

	// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
	CloseImpl()
}

type BaseWorker struct {
	Impl WorkerImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	metaKVClient          metadata.MetaKV

	masterClient *masterClient
	masterID     MasterID

	id            WorkerID
	timeoutConfig TimeoutConfig

	wg    sync.WaitGroup
	errCh chan error

	cancelMu      sync.Mutex
	cancelBgTasks context.CancelFunc

	clock clock.Clock
}

func NewBaseWorker(
	impl WorkerImpl,
	messageHandlerManager p2p.MessageHandlerManager,
	messageSender p2p.MessageSender,
	metaKVClient metadata.MetaKV,
	workerID WorkerID,
	masterID MasterID,
) *BaseWorker {
	return &BaseWorker{
		Impl:                  impl,
		messageHandlerManager: messageHandlerManager,
		messageSender:         messageSender,
		metaKVClient:          metaKVClient,

		masterID:      masterID,
		id:            workerID,
		timeoutConfig: defaultTimeoutConfig,

		errCh: make(chan error, 1),
		clock: clock.New(),
	}
}

func (w *BaseWorker) ID() WorkerID {
	return w.id
}

func (w *BaseWorker) Workload() model.RescUnit {
	return w.Impl.Workload()
}

func (w *BaseWorker) Init(ctx context.Context) error {
	w.masterClient = newMasterClient(
		w.masterID,
		w.id,
		w.messageSender,
		w.metaKVClient,
		w.clock.Mono(),
		func() error {
			return errors.Trace(w.Impl.OnMasterFailover(MasterFailoverReason{
				// TODO support other fail-over reasons
				Code: MasterTimedOut,
			}))
		})

	if err := w.initMessageHandlers(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.masterClient.InitMasterInfoFromMeta(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := w.Impl.InitImpl(ctx); err != nil {
		return errors.Trace(err)
	}

	w.startBackgroundTasks()
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

	if err := w.Impl.Tick(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *BaseWorker) Close(_ctx context.Context) error {
	// Close now accepts a `context.Context` so that it is compatible with Master.
	// TODO think about what is the best design of the Close method.
	w.Impl.CloseImpl()

	w.cancelMu.Lock()
	w.cancelBgTasks()
	w.cancelMu.Unlock()

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	if err := w.messageHandlerManager.Clean(closeCtx); err != nil {
		log.L().Warn("cleaning message handlers failed",
			zap.Error(err))
	}

	w.wg.Wait()
	return nil
}

func (w *BaseWorker) WorkerID() WorkerID {
	return w.id
}

func (w *BaseWorker) MetaKVClient() metadata.MetaKV {
	return w.metaKVClient
}

func (w *BaseWorker) startBackgroundTasks() {
	ctx, cancel := context.WithCancel(context.Background())

	w.cancelMu.Lock()
	w.cancelBgTasks = cancel
	w.cancelMu.Unlock()

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

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.runWatchDog(ctx); err != nil {
			w.onError(err)
		}
	}()
}

func (w *BaseWorker) runHeartbeatWorker(ctx context.Context) error {
	ticker := w.clock.Ticker(w.timeoutConfig.workerHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if err := w.masterClient.SendHeartBeat(ctx, w.clock); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *BaseWorker) runStatusWorker(ctx context.Context) error {
	ticker := w.clock.Ticker(w.timeoutConfig.workerReportStatusInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		status, err := w.Impl.Status()
		if err != nil {
			return errors.Trace(err)
		}
		if err := w.masterClient.SendStatus(ctx, status); err != nil {
			return errors.Trace(err)
		}
	}
}

func (w *BaseWorker) runWatchDog(ctx context.Context) error {
	ticker := w.clock.Ticker(w.timeoutConfig.workerHeartbeatInterval)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		}

		isNormal, err := w.masterClient.CheckMasterTimeout(ctx, w.clock)
		if err != nil {
			return errors.Trace(err)
		}
		if !isNormal {
			return derror.ErrWorkerSuicide.GenWithStackByArgs()
		}
	}
}

func (w *BaseWorker) initMessageHandlers(ctx context.Context) error {
	topic := HeartbeatPongTopic(w.masterClient.MasterID())
	ok, err := w.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&HeartbeatPongMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			msg := value.(*HeartbeatPongMessage)
			log.L().Debug("heartbeat pong received",
				zap.Any("msg", msg))
			w.masterClient.HandleHeartbeat(sender, msg)
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

type masterClient struct {
	mu          sync.RWMutex
	masterID    MasterID
	masterNode  p2p.NodeID
	masterEpoch Epoch

	workerID WorkerID

	messageSender           p2p.MessageSender
	metaKVClient            metadata.MetaKV
	lastMasterAckedPingTime clock.MonotonicTime

	timeoutConfig TimeoutConfig

	onMasterFailOver func() error
}

func newMasterClient(
	masterID MasterID,
	workerID WorkerID,
	messageRouter p2p.MessageSender,
	metaKV metadata.MetaKV,
	initTime clock.MonotonicTime,
	onMasterFailOver func() error,
) *masterClient {
	return &masterClient{
		masterID:                masterID,
		workerID:                workerID,
		messageSender:           messageRouter,
		metaKVClient:            metaKV,
		lastMasterAckedPingTime: initTime,
		timeoutConfig:           defaultTimeoutConfig,
		onMasterFailOver:        onMasterFailOver,
	}
}

func (m *masterClient) InitMasterInfoFromMeta(ctx context.Context) error {
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

func (m *masterClient) refreshMasterInfo(ctx context.Context) error {
	metaClient := NewMetadataClient(m.masterID, m.metaKVClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	m.mu.Lock()
	m.masterNode = masterMeta.NodeID
	if m.masterEpoch < masterMeta.Epoch {
		m.masterEpoch = masterMeta.Epoch
		m.mu.Unlock()
		if err := m.onMasterFailOver(); err != nil {
			return errors.Trace(err)
		}
	} else {
		m.mu.Unlock()
	}
	return nil
}

func (m *masterClient) MasterID() MasterID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.masterID
}

func (m *masterClient) HandleHeartbeat(sender p2p.NodeID, msg *HeartbeatPongMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if msg.Epoch < m.masterEpoch {
		log.L().Info("epoch does not match, ignore stale heartbeat",
			zap.Any("msg", msg),
			zap.Int64("master-epoch", m.masterEpoch))
		return
	}

	if msg.Epoch > m.masterEpoch {
		// We received a heartbeat from a restarted master, we need to record
		// its information.
		// TODO refine the logic of this part
		m.masterEpoch = msg.Epoch
		m.masterNode = sender
	}
	m.lastMasterAckedPingTime = msg.SendTime
}

func (m *masterClient) CheckMasterTimeout(ctx context.Context, clock clock.Clock) (ok bool, err error) {
	m.mu.RLock()
	lastMasterAckedPingTime := m.lastMasterAckedPingTime
	m.mu.RUnlock()

	sinceLastAcked := clock.Mono().Sub(lastMasterAckedPingTime)
	if sinceLastAcked <= 2*m.timeoutConfig.workerHeartbeatInterval {
		return true, nil
	}

	if sinceLastAcked > 2*m.timeoutConfig.workerHeartbeatInterval &&
		sinceLastAcked < m.timeoutConfig.workerTimeoutDuration {

		if err := m.refreshMasterInfo(ctx); err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	return false, nil
}

func (m *masterClient) SendHeartBeat(ctx context.Context, clock clock.Clock) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// We use the monotonic time because we would like to serialize a local timestamp.
	// The timestamp will be returned in a PONG for time-out check, so we need
	// the timestamp to be a local monotonic timestamp, which is not exposed by the
	// standard library `time`.
	sendTime := clock.Mono()
	heartbeatMsg := &HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: m.workerID,
		Epoch:        m.masterEpoch,
	}

	ok, err := m.messageSender.SendToNode(ctx, m.masterNode, HeartbeatPingTopic(m.masterID), heartbeatMsg)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Warn("sending heartbeat ping encountered ErrPeerMessageSendTryAgain")
	}
	return nil
}

func (m *masterClient) SendStatus(ctx context.Context, status WorkerStatus) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	statusUpdateMessage := &StatusUpdateMessage{
		WorkerID: m.workerID,
		Status:   status,
	}
	ok, err := m.messageSender.SendToNode(ctx, m.masterNode, StatusUpdateTopic(m.masterID), statusUpdateMessage)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
			log.L().Warn("sending status update encountered ErrPeerMessageSendTryAgain")
			return nil
		}
	}
	return nil
}
