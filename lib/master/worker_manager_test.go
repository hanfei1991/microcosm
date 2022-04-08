package master

import (
	"context"

	"github.com/hanfei1991/microcosm/lib/config"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type workerManageTestSuite struct {
	manager       *WorkerManager
	meta          metaclient.KVClient
	messageSender p2p.MessageSender
}

func (s *workerManageTestSuite) onWorkerOnline(ctx context.Context, handle WorkerHandle) error {
	return nil
}

func (s *workerManageTestSuite) onWorkerOffline(ctx context.Context, handle WorkerHandle) error {
	return nil
}

func (s *workerManageTestSuite) onWorkerStatusUpdated(ctx context.Context, handle WorkerHandle) error {
	return nil
}

func NewWorkerManageTestSuite() *workerManageTestSuite {
	ret := &workerManageTestSuite{
		meta:          mock.NewMetaMock(),
		messageSender: p2p.NewMockMessageSender(),
	}
	manager := NewWorkerManager("master-1", 1, ret.meta, ret.messageSender, ret.onWorkerOnline,
		ret.onWorkerOffline, ret.onWorkerStatusUpdated, true, config.DefaultTimeoutConfig())
	ret.manager = manager
	return ret
}
