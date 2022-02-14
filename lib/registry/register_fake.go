package registry

import (
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/fake"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
)

type FakeConfig struct{}

// only for test.
func RegisterFake(registry Registry) {
	fakeMasterFactory := NewSimpleWorkerFactory(func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config WorkerConfig) lib.Worker {
		return fake.NewFakeMaster(ctx, id, masterID, config)
	}, &FakeConfig{})
	registry.MustRegisterWorkerType(lib.WorkerTypeFakeMaster, fakeMasterFactory)

	fakeWorkerFactory := NewSimpleWorkerFactory(fake.NewDummyWorker, &FakeConfig{})
	registry.MustRegisterWorkerType(lib.WorkerTypeFakeWorker, fakeWorkerFactory)
}
