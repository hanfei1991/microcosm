package lib

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
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
	id                    MasterID
	messageHandlerManager p2p.MessageHandlerManager

	currentEpoch *atomic.Int64
	workerInfo   map[WorkerID]p2p.NodeID
}

func (m *BaseMaster) Init(ctx context.Context) error {
	if err := m.initMessageHandlers(ctx); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *BaseMaster) InternalID() MasterID {
	return m.id
}

func (m *BaseMaster) checkHeartBeats(ctx context.Context) error {

}

func (m *BaseMaster) initMessageHandlers(ctx context.Context) error {
	topic := workerToMasterHeartbeatTopic(m.id)
	ok, err := m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&workerToMasterHeartbeatMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			heartBeatMsg := value.(*workerToMasterHeartbeatMessage)
			curEpoch := m.currentEpoch.Load()
			if heartBeatMsg.Epoch < m.currentEpoch.Load() {
				log.L().Info("stale message dropped",
					zap.Any("message", heartBeatMsg),
					zap.Int64("cur-epoch", curEpoch))
				return nil
			}
			expectedSender, ok := m.workerInfo[heartBeatMsg.FromWorkerID]
			if !ok || sender != expectedSender {
				log.L().Panic("message from unexpected sender",
					zap.Any("message", heartBeatMsg),
					zap.Any("worker-info", m.workerInfo))
			}
			// TODO handle heartbeat
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
