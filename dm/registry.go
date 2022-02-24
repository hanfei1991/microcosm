package dm

import (
	"github.com/pingcap/tiflow/dm/dm/config"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/pkg/context"
)

const (
	WorkerDMDump lib.WorkerType = 100 + iota
	WorkerDMLoad
	WorkerDMSync
)

func init() {
	dumpFactory := unitWorkerFactory{constructor: newDumpWorker}
	loadFactory := unitWorkerFactory{constructor: newLoadWorker}
	syncFactory := unitWorkerFactory{constructor: newSyncWorker}

	r := registry.GlobalWorkerRegistry()
	r.MustRegisterWorkerType(WorkerDMDump, dumpFactory)
	r.MustRegisterWorkerType(WorkerDMLoad, loadFactory)
	r.MustRegisterWorkerType(WorkerDMSync, syncFactory)
	r.MustRegisterWorkerType(lib.DMJobMaster, jobMasterFactory{})
}

type workerConstructor func(lib.BaseWorker, lib.WorkerConfig) lib.WorkerImpl

type unitWorkerFactory struct {
	constructor workerConstructor
}

func (u unitWorkerFactory) NewWorker(ctx *context.Context, workerID lib.WorkerID, masterID lib.MasterID, config registry.WorkerConfig) (lib.Worker, error) {
	deps := ctx.Dependencies
	base := lib.NewBaseWorker(
		nil,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		workerID,
		masterID,
	)
	worker := u.constructor(base, config)
	base.(*lib.DefaultBaseWorker).Impl = worker
	return worker.(lib.Worker), nil
}

func (u unitWorkerFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}

type jobMasterFactory struct{}

func (j jobMasterFactory) NewWorker(ctx *context.Context, workerID lib.WorkerID, masterID lib.MasterID, config registry.WorkerConfig) (lib.Worker, error) {
	deps := ctx.Dependencies
	baseWorker := lib.NewBaseWorker(
		nil,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		workerID,
		masterID,
	)
	baseMaster := lib.NewBaseMaster(
		ctx,
		nil,
		masterID,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		deps.ExecutorClientManager,
		deps.ServerMasterClient,
	)
	ret := newSubTaskMaster(baseMaster, baseWorker, config)
	baseMaster.(*lib.DefaultBaseMaster).Impl = ret
	baseWorker.(*lib.DefaultBaseWorker).Impl = ret
	return ret, nil
}

func (j jobMasterFactory) DeserializeConfig(configBytes []byte) (registry.WorkerConfig, error) {
	cfg := &config.SubTaskConfig{}
	err := cfg.Decode(string(configBytes), true)
	return cfg, err
}
