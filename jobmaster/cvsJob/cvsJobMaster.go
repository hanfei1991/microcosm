package cvs

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	cvsTask "github.com/hanfei1991/microcosm/executor/cvsTask"
	"github.com/hanfei1991/microcosm/executor/worker"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"go.uber.org/atomic"
)

type Config struct {
	SrcHost string `toml:"srcHost" json:"srcHost"`
	SrcDir  string `toml:"srcDir" json:"srcDir"`
	DstHost string `toml:"dstHost" json:"dstHost"`
	DstDir  string `toml:"dstDir" json:"dstDir"`
}

type SyncFileInfo struct {
	Idx      int    `json:"idx"`
	Location string `json:"loc"`
}

type Status struct {
	*Config `json:"cfg"`

	FileInfos []*SyncFileInfo `json:"files"`
}

type WorkerInfo struct {
	handle     atomic.UnsafePointer // a handler to get information
	needCreate atomic.Bool
}

type JobMaster struct {
	lib.BaseJobMaster
	jobStatus         *Status
	syncFilesInfo     map[int]*WorkerInfo
	counter           int64
	workerID          lib.WorkerID
	status            lib.WorkerStatusCode
	filesNum          int
	statusRateLimiter *rate.Limiter

	workerIDFileIDMap sync.Map
}

func RegisterWorker() {
	constructor := func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config lib.WorkerConfig) lib.WorkerImpl {
		return NewCVSJobMaster(ctx, id, masterID, config)
	}
	factory := registry.NewSimpleWorkerFactory(constructor, &Config{})
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.CvsJobMaster, factory)
}

func NewCVSJobMaster(ctx *dcontext.Context, workerID lib.WorkerID, masterID lib.MasterID, conf lib.WorkerConfig) *JobMaster {
	jm := &JobMaster{}
	jm.workerID = workerID
	jm.jobStatus = &Status{}
	jm.jobStatus.Config = conf.(*Config)
	jm.syncFilesInfo = make(map[int]*WorkerInfo)
	jm.statusRateLimiter = rate.NewLimiter(rate.Every(time.Second*2), 1)
	log.L().Info("new cvs jobmaster ", zap.Any("id", jm.workerID))
	return jm
}

func (jm *JobMaster) InitImpl(ctx context.Context) (err error) {
	log.L().Info("initializing the cvs jobmaster", zap.Any("id", jm.workerID))
	jm.status = lib.WorkerStatusInit
	jm.filesNum, err = jm.listSrcFiles(ctx)
	if err != nil {
		return err
	}
	if jm.filesNum == 0 {
		log.L().Panic("no file found under the folder ", zap.Any("id", jm.workerID))
	}
	log.L().Info("cvs jobmaster list file success", zap.Any("id", jm.workerID), zap.Any("file number", jm.filesNum))
	// todo: store the jobmaster information into the metastore
	for idx := 0; idx < jm.filesNum; idx++ {
		conf := cvsTask.Config{
			SrcHost: jm.jobStatus.SrcHost,
			Idx:     idx,
			DstHost: jm.jobStatus.DstHost,
			DstDir:  jm.jobStatus.DstDir,
		}
		workerID, err := jm.CreateWorker(lib.CvsTask, conf, 10 /* TODO add cost */)
		if err != nil {
			// todo : handle the error case, should recover for failing to create worker
			return err
		}
		jm.jobStatus.FileInfos = append(jm.jobStatus.FileInfos, &SyncFileInfo{Idx: idx})
		jm.syncFilesInfo[idx] = &WorkerInfo{
			handle: *atomic.NewUnsafePointer(unsafe.Pointer(nil)),
		}
		jm.workerIDFileIDMap.Store(workerID, idx)
	}
	jm.status = lib.WorkerStatusNormal
	return nil
}

func (jm *JobMaster) Tick(ctx context.Context) error {
	filesNum := 0
	jm.counter = 0
	if !jm.IsMasterReady() {
		log.L().Info("job is not ready", zap.Any("master id", jm.workerID))
		return nil
	}

	for idx, workerInfo := range jm.syncFilesInfo {
		// check if need to recreate worker
		if workerInfo.needCreate.Load() {
			workerID, err := jm.CreateWorker(lib.CvsTask, getTaskConfig(jm.jobStatus, idx), 10)
			if err != nil {
				log.L().Warn("create worker failed, try next time", zap.Any("master id", jm.workerID), zap.Error(err))
			} else {
				jm.workerIDFileIDMap.Store(workerID, idx)
				workerInfo.needCreate.Store(false)
			}
			continue
		}
		// still awaiting online
		if workerInfo.handle.Load() == nil {
			continue
		}
		// update job status
		handle := *(*lib.WorkerHandle)(workerInfo.handle.Load())
		status := handle.Status()
		if status.Code == lib.WorkerStatusNormal || status.Code == lib.WorkerStatusFinished {
			taskStatus := &cvsTask.Status{}
			err := json.Unmarshal(status.ExtBytes, taskStatus)
			if err != nil {
				return err
			}

			jm.jobStatus.FileInfos[idx].Location = taskStatus.CurrentLoc
			jm.counter += taskStatus.Count

			log.L().Debug("cvs job tmp num ", zap.Any("id", idx), zap.Any("status", string(status.ExtBytes)))
			// todo : store the sync progress into the meta store for each file
			if status.Code == lib.WorkerStatusFinished {
				filesNum++
			}
		} else if status.Code == lib.WorkerStatusError {
			log.L().Error("sync file failed ", zap.Any("idx", idx))
		} else {
			log.L().Info("worker status abnormal", zap.Any("status", status))
		}
	}
	if jm.statusRateLimiter.Allow() {
		log.L().Info("cvs job master status", zap.Any("id", jm.workerID), zap.Int64("counter", jm.counter), zap.Any("status", jm.status))
		err := jm.UpdateJobStatus(ctx, jm.Status())
		if err != nil {
			log.L().Warn("update job status, try next time", zap.Any("master id", jm.workerID), zap.Error(err))
		}
	}
	if filesNum == jm.filesNum {
		jm.status = lib.WorkerStatusFinished
		log.L().Info("cvs job master finished")
		return jm.BaseJobMaster.Exit(ctx, jm.Status(), nil)
	}
	return nil
}

func (jm *JobMaster) OnMasterRecovered(ctx context.Context) (err error) {
	log.L().Info("recovering job master", zap.Any("id", jm.ID()))
	// load self status
	metaClt := lib.NewWorkerMetadataClient(jm.workerID, jm.MetaKVClient())
	status, err := metaClt.Load(ctx, jm.workerID)
	if err != nil {
		return err
	}
	log.L().Info("jobmaster recover from meta", zap.Any("master id", jm.ID()), zap.String("status", string(status.ExtBytes)))
	err = json.Unmarshal(status.ExtBytes, jm.jobStatus)
	if err != nil {
		return err
	}
	for id := range jm.jobStatus.FileInfos {
		info := &WorkerInfo{}
		info.needCreate.Store(true)
		jm.syncFilesInfo[id] = info
	}
	return nil
}

func (jm *JobMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	return nil
}

func (jm *JobMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	// todo : add the worker information to the sync files map
	id, exist := jm.workerIDFileIDMap.Load(worker.ID())
	if !exist {
		log.L().Info("job master recovering and get new worker", zap.Any("id", worker.ID()), zap.Any("master id", jm.ID()))
		if jm.IsMasterReady() {
			log.L().Panic("job master has ready and a new worker has been created, brain split occurs!")
		}
		statusBytes := worker.Status().ExtBytes
		status := cvsTask.Status{}
		err := json.Unmarshal(statusBytes, &status)
		if err != nil {
			// bad json
			return err
		}
		id = status.TaskConfig.Idx
	} else {
		log.L().Info("worker online ", zap.Any("id", worker.ID()), zap.Any("master id", jm.ID()))
	}
	jm.syncFilesInfo[id.(int)].handle.Store(unsafe.Pointer(&worker))
	jm.syncFilesInfo[id.(int)].needCreate.Store(false)
	jm.workerIDFileIDMap.Store(worker.ID(), id.(int))
	return nil
}

func getTaskConfig(jobStatus *Status, id int) *cvsTask.Config {
	return &cvsTask.Config{
		SrcHost:  jobStatus.SrcHost,
		DstHost:  jobStatus.DstHost,
		DstDir:   jobStatus.DstDir,
		StartLoc: jobStatus.FileInfos[id].Location,
		Idx:      id,
	}
}

func (jm *JobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	val, exist := jm.workerIDFileIDMap.Load(worker.ID())
	log.L().Info("on worker offline ", zap.Any(" worker :", worker.ID()))
	if !exist {
		log.L().Panic("bad worker found", zap.Any("message", worker.ID()))
	}
	id := val.(int)
	if derrors.ErrWorkerFinish.Equal(reason) {
		log.L().Info("worker finished", zap.String("worker-id", worker.ID()), zap.Any("status", worker.Status()), zap.Error(reason))
		return nil
	}
	jm.syncFilesInfo[id].needCreate.Store(true)
	jm.syncFilesInfo[id].handle.Store(nil)
	return nil
}

func (jm *JobMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	return nil
}

// CloseImpl is called when the master is being closed
func (jm *JobMaster) CloseImpl(ctx context.Context) error {
	return nil
}

func (jm *JobMaster) ID() worker.RunnableID {
	return jm.workerID
}

func (jm *JobMaster) Workload() model.RescUnit {
	return 2
}

func (jm *JobMaster) OnMasterFailover(reason lib.MasterFailoverReason) error {
	return nil
}

func (jm *JobMaster) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("cvs jobmaster: OnJobManagerFailover", zap.Any("reason", reason))
	return nil
}

func (jm *JobMaster) Status() lib.WorkerStatus {
	status, err := json.Marshal(jm.jobStatus)
	if err != nil {
		log.L().Panic("get status failed", zap.String("id", jm.workerID), zap.Error(err))
	}
	return lib.WorkerStatus{
		Code:     jm.status,
		ExtBytes: status,
	}
}

func (jm *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

func (jm *JobMaster) listSrcFiles(ctx context.Context) (int, error) {
	conn, err := grpc.Dial(jm.jobStatus.SrcHost, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the host  ", zap.Any("message", jm.jobStatus.SrcHost))
		return -1, err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	reply, err := client.ListFiles(ctx, &pb.ListFilesReq{})
	if err != nil {
		log.L().Info(" list the directory failed ", zap.String("id", jm.ID()), zap.Error(err))
		return -1, err
	}
	return int(reply.FileNum), nil
}
