package servermaster

import (
	"context"
	"fmt"
	"sync"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const defaultJobMasterCost = 1

// JobManagerImplV2 is a special job master that manages all the job masters, and notify the offline executor to them.
type JobManagerImplV2 struct {
	*lib.BaseMaster

	mu          sync.Mutex
	jobMasters  map[model.ID]*model.Task
	idAllocator autoid.JobIDAllocator

	messageHandlerManager p2p.MessageHandlerManager
	messageSender         p2p.MessageSender
	metaKVClient          metadata.MetaKV
	executorClientManager client.ExecutorClientManager
	serverMasterClient    client.MasterClient
}

func (jm *JobManagerImplV2) Start(ctx context.Context, metaKV metadata.MetaKV) error {
	return nil
}

func (jm *JobManagerImplV2) PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse {
	panic("not implemented")
}

func (jm *JobManagerImplV2) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	panic("not implemented")
}

// SubmitJob processes "SubmitJobRequest".
func (jm *JobManagerImplV2) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	log.L().Logger.Info("submit job", zap.String("config", string(req.Config)))
	resp := &pb.SubmitJobResponse{}
	var masterConfig *model.JobMaster
	switch req.Tp {
	case pb.JobType_Benchmark:
		id := jm.idAllocator.AllocJobID()
		masterConfig = &model.JobMaster{
			ID:     model.ID(id),
			Tp:     model.Benchmark,
			Config: req.Config,
		}
	default:
		err := errors.ErrBuildJobFailed.GenWithStack("unknown job type", req.Tp)
		resp.Err = errors.ToPBError(err)
		return resp
	}
	// CreateWorker here is to create job master actually
	// TODO: use correct worker type and worker cost
	_, err := jm.BaseMaster.CreateWorker(lib.WorkerType(99), masterConfig, defaultJobMasterCost)
	if err != nil {
		log.L().Error("create job master met error", zap.Error(err))
		resp.Err = errors.ToPBError(err)
		return resp
	}

	return resp
}

// NewJobManagerImplV2 creates a new JobManagerImplV2 instance
func NewJobManagerImplV2(
	ctx context.Context,
	id lib.MasterID,
	msgService *p2p.MessageRPCService,
	clients client.ExecutorClientManager,
	etcdClient *clientv3.Client,
) (*JobManagerImplV2, error) {
	impl := &JobManagerImplV2{
		jobMasters:            make(map[model.ID]*model.Task),
		idAllocator:           autoid.NewJobIDAllocator(),
		messageHandlerManager: msgService.MakeHandlerManager(),
		executorClientManager: clients,
		serverMasterClient:    clients.MasterClient(),
		metaKVClient:          metadata.NewMetaEtcd(etcdClient),
	}
	impl.BaseMaster = lib.NewBaseMaster(
		impl,
		id,
		impl.messageHandlerManager,
		impl.messageSender,
		impl.metaKVClient,
		impl.executorClientManager,
		impl.serverMasterClient,
	)
	err := impl.BaseMaster.Init(ctx)
	if err != nil {
		return nil, err
	}
	return impl, nil
}

// InitImpl implements lib.MasterImpl.InitImpl
func (jm *JobManagerImplV2) InitImpl(ctx context.Context) error {
	// TODO: recover existing job masters from metastore
	return nil
}

// Tick implements lib.MasterImpl.Tick
func (jm *JobManagerImplV2) Tick(ctx context.Context) error {
	return nil
}

// OnMasterRecovered implements lib.MasterImpl.OnMasterRecovered
func (jm *JobManagerImplV2) OnMasterRecovered(ctx context.Context) error {
	return nil
}

// OnWorkerDispatched implements lib.MasterImpl.OnWorkerDispatched
func (jm *JobManagerImplV2) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	fmt.Printf("on worker dispatched!!! error: %+v\n", result)
	if result != nil {
		log.L().Warn("dispatch worker met error", zap.Error(result))
		return nil
	}
	// TODO: add job master info
	return nil
}

// OnWorkerOnline implements lib.MasterImpl.OnWorkerOnline
func (jm *JobManagerImplV2) OnWorkerOnline(worker lib.WorkerHandle) error {
	return nil
}

// OnWorkerOffline implements lib.MasterImpl.OnWorkerOffline
func (jm *JobManagerImplV2) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	return nil
}

// OnWorkerMessage implements lib.MasterImpl.OnWorkerMessage
func (jm *JobManagerImplV2) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	return nil
}

// CloseImpl implements lib.MasterImpl.CloseImpl
func (jm *JobManagerImplV2) CloseImpl(ctx context.Context) error {
	return nil
}
