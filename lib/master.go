package lib

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/servermaster/resource"
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
}

type WorkerHandle interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}) error
	Status() WorkerStatus
	Workload() resource.RescUnit
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

		pool:                  workerpool.NewDefaultAsyncPool(4),
		id:                    id,
	}
}

func (m *BaseMaster) Init(ctx context.Context) error {
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
			// TODO put customized info
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
}

func (m *workerManager) HandleHeartBeat(msg *HeartbeatPingMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.L().Debug("received heartbeat", zap.Any("msg", msg))
	workerInfo, ok := m.workerInfos[msg.FromWorkerID]
	if !ok {
		log.L().Info("discarding heartbeat for non-existing worker",
			zap.Any("msg", msg))
	}
	workerInfo.lastHeartBeatReceiveTime = time.Now()
	workerInfo.lastHeartBeatSendTime = msg.SendTime
	workerInfo.hasPendingHeartbeat = true
}

func (m *workerManager) GetWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	value, ok := m.workerInfos[id]
	if !ok {
		return nil, false
	}
	return value, true
}

func (m *workerManager) PutWorkerInfo(info *WorkerInfo) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := info.ID
	_, exists := m.workerInfos[id]
	return !exists
}

func (m *workerManager) RemoveWorkerInfo(id WorkerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.workerInfos[id]
	delete(m.workerInfos, id)
	return exists
}
