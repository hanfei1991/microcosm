package lib

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Master interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() MasterID
	Close(ctx context.Context) error
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

	// CloseImpl is called when the master is being closed
	CloseImpl(ctx context.Context) error
}

type BaseMaster struct {
	MasterImpl

	messageHandlerManager p2p.MessageHandlerManager
	messageRouter         p2p.MessageRouter
	metaKVClient          metadata.MetaKV
	executorClientManager *client.Manager
	serverMasterClient    *client.MasterClient
	metadataManager       MasterMetadataManager
	workers               *workerManager
	pool                  workerpool.AsyncPool

	// read-only fields
	currentEpoch atomic.Int64
	id           MasterID

	wg    sync.WaitGroup
	errCh chan error
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

		pool: workerpool.NewDefaultAsyncPool(4),
		id:   id,
	}
}

func (m *BaseMaster) Init(ctx context.Context) error {
	m.startBackgroundTasks(ctx)

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

func (m *BaseMaster) startBackgroundTasks(ctx context.Context) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.pool.Run(ctx); err != nil {
			m.OnError(err)
		}
	}()
}

func (m *BaseMaster) OnError(err error) {
	select {
	case m.errCh <- err:
	default:
	}
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

	topic = WorkloadReportTopic(m.id)
	ok, err = m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&WorkloadReportMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			workloadMessage := value.(*WorkloadReportMessage)
			m.workers.UpdateWorkload(workloadMessage)
			return nil
		})
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		log.L().Panic("duplicate handler",
			zap.String("topic", topic))
	}

	topic = StatusUpdateTopic(m.id)
	ok, err = m.messageHandlerManager.RegisterHandler(
		ctx,
		topic,
		&StatusUpdateMessage{},
		func(sender p2p.NodeID, value p2p.MessageValue) error {
			statusUpdateMessage := value.(*StatusUpdateMessage)
			m.workers.UpdateStatus(statusUpdateMessage)
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

func (m *BaseMaster) CreateWorker(ctx context.Context, workerType WorkerType, config []byte) error {
	err := m.pool.Go(ctx, func() {
		// This following API should be refined.
		resp, err := m.serverMasterClient.ScheduleTask(ctx, &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
			Task: &pb.TaskRequest{
				Id: 0,
			},
			// TODO (zixiong) implement the real cost.
			Cost: 10,
		}}},
			// TODO (zixiong) make the timeout configurable
			time.Second*10)
		if err != nil {
			err1 := m.OnWorkerDispatched(nil, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err))
			}
			return
		}
		schedule := resp.GetSchedule()
		if len(schedule) != 1 {
			panic("unreachable")
		}
		executorID := schedule[0].ExecutorId

		executorClient := m.executorClientManager.ExecutorClient(model.ExecutorID(executorID))
		executorResp, err := executorClient.Send(ctx, &client.ExecutorRequest{
			Cmd: client.CmdDispatchTask,
			Req: &pb.DispatchTaskRequest{
				TaskTypeId: int64(workerType),
				TaskConfig: config,
			},
		})
		if err != nil {
			err1 := m.OnWorkerDispatched(nil, errors.Trace(err))
			if err1 != nil {
				m.OnError(errors.Trace(err))
			}
			return
		}
		dispatchTaskResp := executorResp.Resp.(*pb.DispatchTaskResponse)
		errCode := dispatchTaskResp.GetErrorCode()
		if errCode != pb.DispatchTaskErrorCode_OK {
			err1 := m.OnWorkerDispatched(
				nil, errors.Errorf("dispatch worker failed with error code: %d", errCode))
			if err1 != nil {
				m.OnError(errors.Trace(err))
			}
			return
		}

		workerID := WorkerID(dispatchTaskResp.GetWorkerId())
		m.workers.onWorkerCreated(workerID, executorID)
		handle := m.workers.getWorkerHandle(workerID)

		if err := m.OnWorkerDispatched(handle, nil); err != nil {
			m.OnError(errors.Trace(err))
		}
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
