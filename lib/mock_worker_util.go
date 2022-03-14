package lib

// This file provides helper function to let the implementation of WorkerImpl
// can finish its unit tests.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/externalresource"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

func MockBaseWorker(
	workerID WorkerID,
	masterID MasterID,
	workerImpl WorkerImpl,
) *DefaultBaseWorker {
	ctx := dcontext.Background()
	dp := deps.NewDeps()
	params := workerParamListForTest{
		MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
		MessageSender:         p2p.NewMockMessageSender(),
		MetaKVClient:          metadata.NewMetaMock(),
		ResourceProxy:         externalresource.NewMockProxy(workerID),
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
	return ret.(*DefaultBaseWorker)
}

func MockBaseWorkerCheckSendMessage(
	t *testing.T,
	worker *DefaultBaseWorker,
	topic p2p.Topic,
	message interface{}) {
	masterNode := worker.masterClient.MasterNode()
	got, ok := worker.messageSender.(*p2p.MockMessageSender).TryPop(masterNode, topic)
	require.True(t, ok)
	require.Equal(t, message, got)
}

func MockBaseWorkerWaitUpdateStatus(
	t *testing.T,
	worker *DefaultBaseWorker,
) {
	topic := WorkerStatusUpdatedTopic(worker.masterClient.MasterID())
	masterNode := worker.masterClient.MasterNode()
	require.Eventually(t, func() bool {
		_, ok := worker.messageSender.(*p2p.MockMessageSender).TryPop(masterNode, topic)
		return ok
	}, time.Second, 100*time.Millisecond)
}

func MockBaseWorkerPersistResource(
	t *testing.T,
	worker *DefaultBaseWorker,
) {
	// TODO more detailed checks on the method call.
	proxy, err := worker.Resource(context.Background(), "fake-resource")
	require.NoError(t, err)

	proxy.(externalresource.MockProxyWithMasterCli).MockMasterCli.
		Mock.On("UpdateResource", mock.Anything, mock.Anything).
		Return(&pb.UpdateResourceResponse{
			Error: &pb.ResourceError{ErrorCode: pb.ResourceErrorCode_ResourceOK},
		}, nil)
}
