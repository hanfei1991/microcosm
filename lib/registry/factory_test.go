package registry

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/dig"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	extKV "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/resource"
)

type paramList struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	MetaKVClient          metaclient.KVClient
	ResourceProxy         resource.Proxy
}

func makeCtxWithMockDeps(t *testing.T) *dcontext.Context {
	dp := deps.NewDeps()
	err := dp.Provide(func() paramList {
		return paramList{
			MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
			MessageSender:         p2p.NewMockMessageSender(),
			MetaKVClient:          extKV.NewMetaMock(),
			ResourceProxy:         resource.NewMockProxy("makeCtxWithMockDeps"),
		}
	})
	require.NoError(t, err)
	return dcontext.Background().WithDeps(dp)
}

func TestNewSimpleWorkerFactory(t *testing.T) {
	dummyConstructor := func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config WorkerConfig) lib.WorkerImpl {
		return fake.NewDummyWorker(ctx, id, masterID, config)
	}
	fac := NewSimpleWorkerFactory(dummyConstructor, &fake.WorkerConfig{})
	config, err := fac.DeserializeConfig([]byte(`{"target-tick":100}`))
	require.NoError(t, err)
	require.Equal(t, &fake.WorkerConfig{TargetTick: 100}, config)

	ctx := makeCtxWithMockDeps(t)
	newWorker, err := fac.NewWorkerImpl(ctx, "my-worker", "my-master", &fake.WorkerConfig{TargetTick: 100})
	require.NoError(t, err)
	require.IsType(t, &fake.Worker{}, newWorker)
}
