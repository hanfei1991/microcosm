package fake

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// Config represents the job config of fake master
type Config struct {
	JobName     string `json:"job-name"`
	WorkerCount int    `json:"worker-count"`
}

var _ lib.BaseJobMaster = (*Master)(nil)

type Master struct {
	lib.BaseJobMaster

	// workerID stores the ID of the Master AS A WORKER.
	workerID lib.WorkerID

	workerListMu      sync.Mutex
	workerList        []lib.WorkerHandle
	pendingWorkerSet  map[lib.WorkerID]int
	statusRateLimiter *rate.Limiter
	status            map[lib.WorkerID]int64
	config            *Config
}

func (m *Master) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("FakeMaster: OnJobManagerFailover", zap.Any("reason", reason))
	return nil
}

func (m *Master) IsJobMasterImpl() {
	panic("unreachable")
}

func (m *Master) ID() worker.RunnableID {
	return m.workerID
}

func (m *Master) Workload() model.RescUnit {
	return 0
}

func (m *Master) InitImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Init", zap.Any("config", m.config))
	m.workerList = make([]lib.WorkerHandle, m.config.WorkerCount)
	return m.createWorkers()
}

func (m *Master) createWorkers() error {
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()
OUT:
	for i, handle := range m.workerList {
		if handle == nil {
			for _, idx := range m.pendingWorkerSet {
				if idx == i {
					continue OUT
				}
			}

			workerID, err := m.CreateWorker(lib.FakeTask, &Config{}, 1)
			if err != nil {
				return errors.Trace(err)
			}
			log.L().Info("CreateWorker called",
				zap.Int("index", i),
				zap.String("worker-id", workerID))
			m.pendingWorkerSet[workerID] = i
		}
	}
	return nil
}

func (m *Master) Tick(ctx context.Context) error {
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()
	for _, worker := range m.workerList {
		if worker != nil {
			status := worker.Status()
			dws := &dummyWorkerStatus{}
			if status.ExtBytes != nil {
				err := dws.Unmarshal(status.ExtBytes)
				if err != nil {
					return err
				}
			}
			m.status[worker.ID()] = dws.Tick
		}
	}
	if m.statusRateLimiter.Allow() {
		log.L().Info("FakeMaster: Tick", zap.Any("status", m.status))
		err := m.BaseJobMaster.UpdateJobStatus(ctx, m.Status())
		if derrors.ErrWorkerUpdateStatusTryAgain.Equal(err) {
			log.L().Warn("update status try again later", zap.String("error", err.Error()))
			return nil
		}
		return err
	}

	return nil
}

func (m *Master) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("FakeMaster: OnMasterRecovered")
	return nil
}

func (m *Master) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	if result != nil {
		log.L().Error("FakeMaster: OnWorkerDispatched", zap.Error(result))
		return errors.Trace(result)
	}

	log.L().Info("FakeMaster: OnWorkerDispatched",
		zap.String("worker-id", worker.ID()),
		zap.Error(result))

	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()

	idx, ok := m.pendingWorkerSet[worker.ID()]
	if !ok {
		log.L().Panic("OnWorkerDispatched is called with an unknown workerID",
			zap.String("worker-id", worker.ID()))
	}
	delete(m.pendingWorkerSet, worker.ID())
	m.workerList[idx] = worker

	return nil
}

func (m *Master) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("FakeMaster: OnWorkerOnline",
		zap.String("worker-id", worker.ID()))

	return nil
}

func (m *Master) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("FakeMaster: OnWorkerOffline",
		zap.String("worker-id", worker.ID()),
		zap.Error(reason))

	// TODO handle offlined workers
	return nil
}

func (m *Master) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("FakeMaster: OnWorkerMessage",
		zap.String("topic", topic),
		zap.Any("message", message))
	return nil
}

func (m *Master) CloseImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Close", zap.Stack("stack"))
	return nil
}

func (m *Master) OnMasterFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("FakeMaster: OnMasterFailover", zap.Stack("stack"))
	return nil
}

func (m *Master) Status() lib.WorkerStatus {
	bytes, err := json.Marshal(m.status)
	if err != nil {
		log.L().Panic("unexpected marshal error", zap.Error(err))
	}
	return lib.WorkerStatus{
		Code:     lib.WorkerStatusNormal,
		ExtBytes: bytes,
	}
}

func NewFakeMaster(ctx *dcontext.Context, workerID lib.WorkerID, masterID lib.MasterID, config lib.WorkerConfig) *Master {
	log.L().Info("new fake master", zap.Any("config", config))
	ret := &Master{
		pendingWorkerSet:  make(map[lib.WorkerID]int),
		config:            config.(*Config),
		statusRateLimiter: rate.NewLimiter(rate.Every(time.Second*3), 1),
		status:            make(map[lib.WorkerID]int64),
	}
	return ret
}
