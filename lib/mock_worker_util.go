package lib

// This file provides helper function to let the implementation of WorkerImpl
// can finish its unit tests.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib/statusutil"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kv/mockclient"
	dorm "github.com/hanfei1991/microcosm/pkg/meta/orm"
	libModel "github.com/hanfei1991/microcosm/pkg/meta/orm/model"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type BaseWorkerForTesting struct {
	*DefaultBaseWorker
	Broker *broker.LocalBroker
}

func MockBaseWorker(
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	workerImpl WorkerImpl,
) *BaseWorkerForTesting {
	ctx := dcontext.Background()
	dp := deps.NewDeps()

	// TODO: mock?
	mcli, mock, err := dorm.NewMockMetaOpsClient()
	if err != nil {
		panic(err)
	}

	resourceBroker := broker.NewBrokerForTesting("executor-1")
	params := workerParamListForTest{
		MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
		MessageSender:         p2p.NewMockMessageSender(),
		FrameMetaClient:       mcli,
		UserRawKVClient:       mockkv.NewMetaMock(),
		ResourceBroker:        resourceBroker,
	}
	err := dp.Provide(func() workerParamListForTest {
		return params
	})
	if err != nil {
		panic(err)
	}
	ctx = ctx.WithDeps(dp)

	ret := NewBaseWorker(
		ctx,
		workerImpl,
		workerID,
		masterID)
	return &BaseWorkerForTesting{
		ret.(*DefaultBaseWorker),
		resourceBroker,
	}
}

func MockBaseWorkerCheckSendMessage(
	t *testing.T,
	worker *DefaultBaseWorker,
	topic p2p.Topic,
	message interface{},
) {
	masterNode := worker.masterClient.MasterNode()
	got, ok := worker.messageSender.(*p2p.MockMessageSender).TryPop(masterNode, topic)
	require.True(t, ok)
	require.Equal(t, message, got)
}

func MockBaseWorkerWaitUpdateStatus(
	t *testing.T,
	worker *DefaultBaseWorker,
) {
	topic := statusutil.WorkerStatusTopic(worker.masterClient.MasterID())
	masterNode := worker.masterClient.MasterNode()
	require.Eventually(t, func() bool {
		_, ok := worker.messageSender.(*p2p.MockMessageSender).TryPop(masterNode, topic)
		return ok
	}, time.Second, 100*time.Millisecond)
}
