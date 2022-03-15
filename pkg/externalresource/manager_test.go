package externalresource

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
)

type managerTestSuite struct {
	manager *Manager
	mockKV  *mock.MetaMock
}

func (s *managerTestSuite) Stop() {
	s.manager.Stop()
}

func newManagerTestSuite(t *testing.T) *managerTestSuite {
	kv := mock.NewMetaMock()
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

	resourceKey := adapter.ResourceKeyAdapter.Encode("/local/abc")
	metaStr, err := json.Marshal(&model.ResourceMeta{
		ID:        "/local/abc",
		LeaseType: model.LeaseTypeWorker,
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
	})
	require.NoError(t, err)

	err = suite.manager.CreateResource(
		ctx, "/local/abc", "job-1", "executor-1", "worker-1")
	require.NoError(t, err)

	resp, err := suite.mockKV.Get(ctx, resourceKey)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, resp.Kvs[0].Value, metaStr)

	err = suite.manager.UpdateResource(ctx, "/local/abc", "worker-2", model.LeaseTypeJob)
	require.NoError(t, err)

	execID, err := suite.manager.GetExecutorConstraint(ctx, "/local/abc")
	require.NoError(t, err)
	require.Equal(t, model.ExecutorID("executor-1"), execID)
}
