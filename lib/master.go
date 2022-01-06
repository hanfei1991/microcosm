package lib

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type Master interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	InternalID() MasterID
}

type master interface {
	// Master defines all exported methods
	Master

	// The following methods are "virtual" methods that can be
	// overridden.
	checkHeartBeats(ctx context.Context) error
	onMessage(ctx context.Context, workerID WorkerID) error

	createWorker(ctx context.Context) error
}

type BaseMaster struct {
	master

	messageHandlerManager p2p.MessageHandlerManager
	messageRouter         p2p.MessageRouter
	metaKVClient          metadata.MetaKV
	executorClientManager *client.Manager
	epochGenerator        EpochGenerator

	workers *workerManager

	// read-only fields
	currentEpoch epoch
	id           MasterID
}

func (m *BaseMaster) Init(ctx context.Context) error {
	if err := m.initMessageHandlers(ctx); err != nil {
		return errors.Trace(err)
	}

	if err := m.initMetadata(ctx); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *BaseMaster) InternalID() MasterID {
	return m.id
}

func (m *BaseMaster) createWorker(ctx context.Context) error {
	
}

func (m *BaseMaster) initMessageHandlers(ctx context.Context) error {
	topic := workerToMasterHeartbeatTopic(m.id)
	ok, err := m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&workerToMasterHeartbeatMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			heartBeatMsg := value.(*workerToMasterHeartbeatMessage)
			if heartBeatMsg.Epoch < m.currentEpoch {
				log.L().Info("stale message dropped",
					zap.Any("message", heartBeatMsg),
					zap.Int64("cur-epoch", m.currentEpoch))
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

func (m *BaseMaster) initMetadata(ctx context.Context) error {
	key := adapter.JobKeyAdapter.Encode(string(m.InternalID()))
	epoch, err := m.epochGenerator.NewEpoch(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	masterInfo := &MasterMetaKVData{
		ID: m.id,
		// TODO get Addr and NodeID from ctx
		Addr:   "",
		NodeID: "",
		Epoch:  epoch,
	}
	log.L().Info("initializing master to metastore",
		zap.Any("info", masterInfo))

	jsonBytes, err := json.Marshal(masterInfo)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = m.metaKVClient.Put(ctx, key, string(jsonBytes))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// workerManager is for private use by BaseMaster.
type workerManager struct {
	mu            sync.Mutex
	initStartTime time.Time
	workerInfos   map[WorkerID]*WorkerInfo

	// read-only
	masterEpoch epoch
	masterID    MasterID
}

func (m *workerManager) Initialized(ctx context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
		reply := &masterToWorkerHeartbeatMessage{
			SendTime: workerInfo.lastHeartBeatSendTime,
			ReplyTime: time.Now(),
			Epoch: m.masterEpoch,
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
		_, err := msgClient.TrySendMessage(ctx, masterToWorkerHeartbeatTopic(m.masterID), reply)
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

func (m *workerManager) HandleHeartBeat(msg *workerToMasterHeartbeatMessage) {
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
