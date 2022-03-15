package externalresource

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	mock "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

type managerTestSuite struct {
	manager *Manager
	mockKV  *mock.MockKVClient
}

func (s *managerTestSuite) Stop() {
	s.manager.Stop()
}

func newManagerTestSuite(t *testing.T) *managerTestSuite {
	kv := mock.NewMockKVClient(gomock.NewController(t))
	return &managerTestSuite{
		manager: NewManager(kv),
		mockKV:  kv,
	}
}

func TestManagerBasics(t *testing.T) {
	suite := newManagerTestSuite(t)
	defer suite.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resourceKey := adapter.ResourceKeyAdapter.Encode("/localfs/abc")
	suite.mockKV.EXPECT().Get(gomock.Any(), resourceKey).Return(
		&metaclient.GetResponse{
			Kvs: []*metaclient.KeyValue{},
		}, nil,
	)

	metaStr, err := json.Marshal(&model.ResourceMeta{
		ID:        "/localfs/abc",
		LeaseType: model.LeaseTypeWorker,
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
	})
	require.NoError(t, err)

	suite.mockKV.EXPECT().Put(gomock.Any(), resourceKey, string(metaStr)).Return(
		&metaclient.PutResponse{}, nil,
	)
	err = suite.manager.CreateResource(
		ctx, "/localfs/abc", "job-1", "executor-1", "worker-1")
	require.NoError(t, err)
}
