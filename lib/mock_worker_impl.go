package lib

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"

	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type mockWorkerImpl struct {
	mu sync.Mutex
	mock.Mock

	*DefaultBaseWorker
	id WorkerID

	messageHandlerManager *p2p.MockMessageHandlerManager
	messageSender         *p2p.MockMessageSender
	metaKVClient          *metadata.MetaMock

	failoverCount atomic.Int64
}

//nolint:unparam
func newMockWorkerImpl(workerID WorkerID, masterID MasterID) *mockWorkerImpl {
	ret := &mockWorkerImpl{
		id: workerID,
	}
	ret.DefaultBaseWorker = MockBaseWorker(workerID, masterID, ret)
	ret.messageHandlerManager = ret.DefaultBaseWorker.messageHandlerManager.(*p2p.MockMessageHandlerManager)
	ret.messageSender = ret.DefaultBaseWorker.messageSender.(*p2p.MockMessageSender)
	ret.metaKVClient = ret.DefaultBaseWorker.metaKVClient.(*metadata.MetaMock)
	return ret
}

func (w *mockWorkerImpl) Init(ctx context.Context) error {
	return errors.Trace(w.DefaultBaseWorker.Init(ctx))
}

func (w *mockWorkerImpl) Poll(ctx context.Context) error {
	return errors.Trace(w.DefaultBaseWorker.Poll(ctx))
}

func (w *mockWorkerImpl) GetWorkerStatusExtTypeInfo() interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	args := w.Called()
	return args.Get(0)
}

func (w *mockWorkerImpl) OnMasterFailover(reason MasterFailoverReason) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.failoverCount.Add(1)

	args := w.Called(reason)
	return args.Error(0)
}

func (w *mockWorkerImpl) Close(ctx context.Context) error {
	return errors.Trace(w.DefaultBaseWorker.Close(ctx))
}
