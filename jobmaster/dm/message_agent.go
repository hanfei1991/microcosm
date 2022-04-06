package dm

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/jobmaster/dm/config"
	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dmpkg "github.com/hanfei1991/microcosm/pkg/dm"
	"github.com/pingcap/errors"
)

var (
	DefaultMessageTimeOut = time.Second * 2
	DefaultRequestTimeOut = time.Second * 30
)

type MasterImpl interface {
	// for create worker
	CreateWorker(workerType lib.WorkerType, config lib.WorkerConfig, cost model.RescUnit) (lib.WorkerID, error)
	// for operate-task
	CurrentEpoch() lib.Epoch
	// for topic
	JobMasterID() lib.MasterID
}

type Sender interface {
	ID() lib.WorkerID
	SendMessage(ctx context.Context, topic string, message interface{}, nonblocking bool) error
}

type MessageAgent struct {
	masterImpl  MasterImpl
	clocker     clock.Clock
	messageImpl *dmpkg.MessageImpl
	// for stop a task
	id lib.WorkerID
	// taskID -> Sender(WorkerHandle)
	senders sync.Map
}

func NewMessageAgent(id lib.WorkerID, masterImpl MasterImpl) *MessageAgent {
	return &MessageAgent{
		masterImpl:  masterImpl,
		clocker:     clock.New(),
		messageImpl: dmpkg.NewMessageImpl(),
	}
}

func (agent *MessageAgent) UpdateWorkerHandle(taskID string, sender Sender) {
	if sender == nil {
		agent.senders.Delete(taskID)
	} else {
		agent.senders.Store(taskID, sender)
	}
}

func (agent *MessageAgent) CreateWorker(ctx context.Context, taskID string, workerType lib.WorkerType, taskCfg *config.TaskCfg) (lib.WorkerID, error) {
	if _, ok := agent.senders.Load(taskID); ok {
		return "", errors.Errorf("worker for task %s already exist", taskID)
	}
	return agent.masterImpl.CreateWorker(workerType, taskCfg, 1)
}

func (agent *MessageAgent) DestroyWorker(ctx context.Context, taskID lib.WorkerID, workerID lib.WorkerID) error {
	v, ok := agent.senders.Load(taskID)
	if !ok {
		return errors.Errorf("worker for task %s not exist", taskID)
	}

	sender := v.(Sender)
	if sender.ID() != workerID {
		return errors.Errorf("worker for task %s mismatch: want %s, get %s", taskID, workerID, sender.ID())
	}

	topic := lib.WorkerStatusChangeRequestTopic(agent.id, workerID)
	message := &lib.StatusChangeRequest{
		SendTime:     agent.clocker.Mono(),
		FromMasterID: agent.id,
		Epoch:        agent.masterImpl.CurrentEpoch(),
		ExpectState:  libModel.WorkerStatusStopped,
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultMessageTimeOut)
	defer cancel()
	return agent.messageImpl.SendMessage(ctx, topic, message, sender)
}

func (agent *MessageAgent) OperateTask(ctx context.Context, taskID string, stage metadata.TaskStage) error {
	v, ok := agent.senders.Load(taskID)
	if !ok {
		return errors.Errorf("worker for task %s not exist", taskID)
	}

	topic := dmpkg.OperateTaskMessageTopic(agent.masterImpl.JobMasterID(), taskID)
	message := &dmpkg.OperateTaskMessage{
		TaskID: taskID,
		Stage:  stage,
	}

	ctx, cancel := context.WithTimeout(ctx, DefaultMessageTimeOut)
	defer cancel()
	return agent.messageImpl.SendMessage(ctx, topic, message, v.(Sender))
}

func (agent *MessageAgent) OnWorkerMessage(message interface{}) error {
	return agent.messageImpl.OnResponse(message.(dmpkg.MessageWithID))
}
