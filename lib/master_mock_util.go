package lib

// This file provides helper function to let the implementation of MasterImpl
// can finish its unit tests.

import (
	"encoding/json"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

func MockBaseMaster(id MasterID, masterImpl MasterImpl) *BaseMaster {
	ret := NewBaseMaster(
		// ctx is nil for now
		// TODO refine this
		nil,
		masterImpl,
		id,
		p2p.NewMockMessageHandlerManager(),
		p2p.NewMockMessageSender(),
		metadata.NewMetaMock(),
		client.NewClientManager(),
		&client.MockServerMasterClient{})

	return ret
}

func MockBaseMasterCreateWorker(
	master *BaseMaster,
	workerType WorkerType,
	config WorkerConfig,
	cost model.RescUnit,
	masterID MasterID,
	workerID WorkerID,
	executorID model.ExecutorID,
) error {
	master.timeoutConfig.masterHeartbeatCheckLoopInterval = time.Millisecond * 10
	master.uuidGen = uuid.NewMock()

	expectedSchedulerReq := &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
		Task: &pb.TaskRequest{
			Id: 0,
		},
		Cost: int64(cost),
	}}}
	master.serverMasterClient.(*client.MockServerMasterClient).On(
		"ScheduleTask",
		mock.Anything,
		expectedSchedulerReq,
		mock.Anything).Return(
		&pb.TaskSchedulerResponse{
			Schedule: map[int64]*pb.ScheduleResult{
				0: {
					ExecutorId: string(executorID),
				},
			},
		}, nil)

	mockExecutorClient := &client.MockExecutorClient{}
	err := master.executorClientManager.(*client.Manager).AddExecutorClient(executorID, mockExecutorClient)
	if err != nil {
		return err
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}

	mockExecutorClient.On("Send",
		mock.Anything,
		&client.ExecutorRequest{
			Cmd: client.CmdDispatchTask,
			Req: &pb.DispatchTaskRequest{
				TaskTypeId: int64(workerType),
				TaskConfig: configBytes,
				MasterId:   string(masterID),
				WorkerId:   string(workerID),
			},
		}).Return(&client.ExecutorResponse{Resp: &pb.DispatchTaskResponse{
		ErrorCode: 1,
	}}, nil)

	master.uuidGen.(*uuid.MockGenerator).Push(string(workerID))

	return nil
}
