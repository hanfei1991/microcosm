package registry

import (
	"reflect"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/lib"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
)

type WorkerConfig = lib.WorkerConfig

type Registry interface {
	MustRegisterWorkerType(tp lib.WorkerType, factory WorkerFactory)
	RegisterWorkerType(tp lib.WorkerType, factory WorkerFactory) (ok bool)
	CreateWorker(
		ctx *dcontext.Context,
		tp lib.WorkerType,
		workerID lib.WorkerID,
		masterID lib.MasterID,
		config []byte,
	) (lib.Worker, error)
}

type registryImpl struct {
	mu         sync.RWMutex
	factoryMap map[lib.WorkerType]WorkerFactory
}

func NewRegistry() Registry {
	return &registryImpl{
		factoryMap: make(map[lib.WorkerType]WorkerFactory),
	}
}

func (r *registryImpl) MustRegisterWorkerType(tp lib.WorkerType, factory WorkerFactory) {
	if ok := r.RegisterWorkerType(tp, factory); !ok {
		log.L().Panic("duplicate worker type", zap.Int64("worker-type", int64(tp)))
	}
	log.L().Info("register worker", zap.Int64("worker-type", int64(tp)))
}

func (r *registryImpl) RegisterWorkerType(tp lib.WorkerType, factory WorkerFactory) (ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factoryMap[tp]; exists {
		return false
	}
	r.factoryMap[tp] = factory
	return true
}

func (r *registryImpl) CreateWorker(
	ctx *dcontext.Context,
	tp lib.WorkerType,
	workerID lib.WorkerID,
	masterID lib.MasterID,
	configBytes []byte,
) (lib.Worker, error) {
	factory, ok := r.getWorkerFactory(tp)
	if !ok {
		return nil, derror.ErrWorkerTypeNotFound.GenWithStackByArgs(tp)
	}

	config, err := factory.DeserializeConfig(configBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	impl, err := factory.NewWorkerImpl(ctx, workerID, masterID, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if implHasMember(impl, nameOfBaseWorker) {
		base := lib.NewBaseWorker(
			ctx,
			impl,
			workerID,
			masterID,
		)
		setImplMember(impl, nameOfBaseWorker, base)
		return base, nil
	}
	if implHasMember(impl, nameOfBaseJobMaster) {
		base := lib.NewBaseJobMaster(
			ctx,
			impl.(lib.JobMasterImpl),
			masterID,
			workerID,
		)
		setImplMember(impl, nameOfBaseJobMaster, base)
		return base, nil
	}
	log.L().Panic("wrong use of CreateWorker",
		zap.String("reason", "impl has no member BaseWorker or BaseJobMaster"),
		zap.Any("workerType", tp))
	return nil, nil
}

func (r *registryImpl) getWorkerFactory(tp lib.WorkerType) (factory WorkerFactory, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok = r.factoryMap[tp]
	return
}

var (
	nameOfBaseWorker    = getTypeNameOfVarPtr(new(lib.BaseWorker))
	nameOfBaseJobMaster = getTypeNameOfVarPtr(new(lib.BaseJobMaster))
)

func getTypeNameOfVarPtr(v interface{}) string {
	return reflect.TypeOf(v).Elem().Name()
}

func implHasMember(impl interface{}, memberName string) bool {
	defer func() {
		if v := recover(); v != nil {
			log.L().Panic("wrong use of implHasMember",
				zap.Any("reason", v))
		}
	}()
	return reflect.ValueOf(impl).Elem().FieldByName(memberName) != reflect.Value{}
}

func setImplMember(impl interface{}, memberName string, value interface{}) {
	defer func() {
		if v := recover(); v != nil {
			log.L().Panic("wrong use of setImplMember",
				zap.Any("reason", v))
		}
	}()
	reflect.ValueOf(impl).Elem().FieldByName(memberName).Set(reflect.ValueOf(value))
}
