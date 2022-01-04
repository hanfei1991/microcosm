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
	workerInfoMap sync.Map // stores WorkerInfo
	initStartTime time.Time
}

func (m *workerManager) Initialized(ctx context.Context) (bool, error) {
	if m.initStartTime.IsZero() {
		m.initStartTime = time.Now()
	}
	if time.Since(m.initStartTime) > workerTimeoutDuration+workerTimeoutGracefulDuration {
		return true, nil
	}
	return false, nil
}

func (m *workerManager) Tick(ctx context.Context) {
	// respond to worker heartbeats
}

func (m *workerManager) HandleHeartBeat(msg *workerToMasterHeartbeatMessage) {
	log.L().Debug("received heartbeat", zap.Any("msg", msg))

}

func (m *workerManager) GetWorkerInfo(id WorkerID) (*WorkerInfo, bool) {
	value, ok := m.workerInfoMap.Load(id)
	if !ok {
		return nil, false
	}
	return value.(*WorkerInfo), false
}

func (m *workerManager) PutWorkerInfo(info *WorkerInfo) bool {
	id := info.ID
	_, exists := m.workerInfoMap.LoadOrStore(id, info)
	return !exists
}

func (m *workerManager) RemoveWorkerInfo(id WorkerID) bool {
	_, exists := m.workerInfoMap.LoadAndDelete(id)
	return exists
}
