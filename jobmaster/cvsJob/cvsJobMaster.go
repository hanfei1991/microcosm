package cvs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type Config struct {
	SrcHost string `toml:"srcHost" json:"srcHost"`
	SrcDir  string `toml:"srcDir" json:"srcDir"`
	DstHost string `toml:"dstHost" json:"dstHost"`
	DstDir  string `toml:"dstDir" json:"dstDir"`
}

type workerInfo struct {
	id               lib.WorkerID
	idx              int
	curLoc           string
	handle           atomic.Value
	waitingRecover   bool
	waitAckStartTime time.Time
}

type errorInfo struct {
	info string
}

func (e *errorInfo) Error() string {
	return e.info
}

type JobMaster struct {
	lib.BaseJobMaster
	syncInfo          *Config
	mu                sync.Mutex
	syncFilesInfo     map[lib.WorkerID]*workerInfo
	counter           int64
	workerID          lib.WorkerID
	status            lib.WorkerStatusCode
	filesNum          int
	statusRateLimiter *rate.Limiter
	clocker           clock.Clock
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
	jm.syncInfo = conf.(*Config)
	jm.syncFilesInfo = make(map[lib.WorkerID]*workerInfo)
	jm.statusRateLimiter = rate.NewLimiter(rate.Every(time.Second*2), 1)
	jm.clocker = clock.New()
	log.L().Info("new cvs jobmaster ", zap.Any("id", jm.workerID))
	return jm
}

func (jm *JobMaster) InitImpl(ctx context.Context) (err error) {
	if jm.syncInfo.DstHost == jm.syncInfo.SrcHost && jm.syncInfo.SrcDir == jm.syncInfo.DstDir {
		return &errorInfo{info: "bad configure file ,make sure the source address is not the same as the destination"}
	}
	log.L().Info("initializing the cvs jobmaster  ", zap.Any("id :", jm.workerID))
	jm.status = lib.WorkerStatusInit
	jm.filesNum, err = jm.listSrcFiles(ctx)
	if err != nil {
		return err
	}
	if jm.filesNum == 0 {
		log.L().Info("no file found under the folder ", zap.Any("message", jm.syncInfo.DstDir))
	}
	log.L().Info(" cvs jobmaster list file success", zap.Any("id :", jm.workerID), zap.Any(" file number :", jm.filesNum))
	// todo: store the jobmaster information into the metastore
	for idx := 0; idx < jm.filesNum; idx++ {
		conf := cvsTask.Config{SrcHost: jm.syncInfo.SrcHost, Idx: idx, DstHost: jm.syncInfo.DstHost, DstDir: jm.syncInfo.DstDir, StartLoc: "0"}
		workerID, err := jm.CreateWorker(lib.CvsTask, conf, 10 /* TODO add cost */)
		if err != nil {
			// todo : handle the error case, should recover for failing to create worker
			return err
		}
		jm.syncFilesInfo[workerID] = &workerInfo{id: workerID, idx: idx}
	}
	return nil
}

func (jm *JobMaster) Tick(ctx context.Context) error {
	filesNum := 0
	jm.counter = 0
	if jm.status != lib.WorkerStatusNormal {
		err := jm.UpdateJobStatus(ctx, jm.Status())
		if err != nil {
			log.L().Warn("update job status failed", zap.Any("id", jm.workerID), zap.Error(err))
		} else {
			jm.status = lib.WorkerStatusNormal
		}
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()
	var oldWorkers, newWorkers []*workerInfo
	for _, worker := range jm.syncFilesInfo {
		if worker.waitingRecover {
			duration := jm.clocker.Since(worker.waitAckStartTime)
			if duration > 16*time.Second {
				wid, err := jm.CreateWorker(lib.CdcTask, getTaskConfig(worker, jm.syncInfo), 10)
				if err != nil {
					return err
				}
				oldWorkers = append(oldWorkers, worker)
				newWorkers = append(newWorkers, &workerInfo{
					id:  wid,
					idx: worker.idx,
				})
			}
		}
		if worker.handle.Load() == nil {
			continue
		}
		status := worker.handle.Load().(lib.WorkerHandle).Status()
		if status.Code == lib.WorkerStatusNormal || status.Code == lib.WorkerStatusFinished {
			taskStatus := &cvsTask.Status{}
			err := json.Unmarshal(status.ExtBytes, taskStatus)
			if err != nil {
				return err
			}
			worker.curLoc = taskStatus.CurrentLoc
			jm.counter += taskStatus.Count
			log.L().Debug("cvs job tmp num ", zap.Any("id", worker.handle.Load().(lib.WorkerHandle).ID()), zap.Any("status", string(status.ExtBytes)))
			// todo : store the sync progress into the meta store for each file
			if status.Code == lib.WorkerStatusFinished {
				filesNum++
			}
		} else if status.Code == lib.WorkerStatusError {
			log.L().Error("sync file failed ", zap.Any("idx", worker.idx))
		} else {
			log.L().Info("worker status abnormal", zap.Any("status", status))
		}
	}
	if jm.statusRateLimiter.Allow() {
		log.L().Info("cvs job master status", zap.Any("id", jm.workerID), zap.Int64("counter", jm.counter), zap.Any("status", jm.status))
	}
	if filesNum == jm.filesNum {
		jm.status = lib.WorkerStatusFinished
		log.L().Info("cvs job master finished")
		return jm.BaseJobMaster.Exit(ctx, jm.Status(), nil)
	}
	for _, new := range newWorkers {
		jm.syncFilesInfo[new.id] = new
	}
	for _, old := range oldWorkers {
		jm.syncFilesInfo[old.id] = old
	}
	return nil
}

func workInfoFromStatus(stats *cvsTask.Status, id lib.WorkerID) *workerInfo {
	return &workerInfo{
		id:     id,
		idx:    stats.TaskConfig.Idx,
		curLoc: stats.CurrentLoc,
	}
}

func (jm *JobMaster) OnMasterRecovered(ctx context.Context) (err error) {
	log.L().Info("recovering job master", zap.Any("id", jm.ID()))
	filesNum, err := jm.listSrcFiles(ctx)
	if err != nil {
		return err
	}
	aliveMap := make(map[int]*workerInfo)
	for i := 0; i < filesNum; i++ {
		aliveMap[i] = &workerInfo{
			idx: i,
		}
	}
	metaClt := lib.NewWorkerMetadataClient(jm.workerID, jm.MetaKVClient())
	workerMaps, err := metaClt.LoadAllWorkers(ctx)
	if err != nil {
		return err
	}
	for workerID, status := range workerMaps {
		taskStatus := &cvsTask.Status{}
		err := json.Unmarshal(status.ExtBytes, taskStatus)
		if err != nil {
			return err
		}
		idx := taskStatus.TaskConfig.Idx
		oldWorker, ok := aliveMap[idx]
		if !ok {
			// this file idx has finished
			continue
		}
		if status.Code != lib.WorkerStatusFinished {
			newWorker := workInfoFromStatus(taskStatus, workerID)
			if newWorker.curLoc > oldWorker.curLoc {
				aliveMap[idx] = newWorker
			}
		} else {
			delete(aliveMap, idx)
		}
	}
	jm.syncFilesInfo = make(map[string]*workerInfo)
	for fileID, worker := range aliveMap {
		if worker.id == "" {
			log.L().Info("recovering file has no worker, will create new ones", zap.Any("file", fileID), zap.Any("id", jm.ID()))
			conf := cvsTask.Config{
				SrcHost:  jm.syncInfo.SrcHost,
				DstHost:  jm.syncInfo.DstHost,
				DstDir:   jm.syncInfo.DstDir,
				StartLoc: worker.curLoc,
				Idx:      worker.idx,
			}
			workerID, err := jm.CreateWorker(lib.CvsTask, conf, 10)
			if err != nil {
				return err
			}
			jm.syncFilesInfo[workerID] = worker
		} else {
			log.L().Info("recovering file has worker, will create wait hb", zap.Any("file", fileID), zap.Any("id", jm.ID()))
			worker.waitAckStartTime = jm.clocker.Now()
			worker.waitingRecover = true
			jm.syncFilesInfo[worker.id] = worker
		}
	}
	jm.filesNum = len(jm.syncFilesInfo)
	return nil
}

func (jm *JobMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	return nil
}

func (jm *JobMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	// todo : add the worker information to the sync files map
	jm.mu.Lock()
	defer jm.mu.Unlock()
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	if !exist {
		log.L().Panic("phantom worker found", zap.Any("id", worker.ID()))
	} else {
		log.L().Info("worker online ", zap.Any("id", worker.ID()))
	}
	syncInfo.handle.Store(worker)
	syncInfo.waitingRecover = false
	return nil
}

func getTaskConfig(info *workerInfo, jobConfig *Config) *cvsTask.Config {
	return &cvsTask.Config{
		SrcHost:  jobConfig.SrcHost,
		DstHost:  jobConfig.DstHost,
		DstDir:   jobConfig.DstDir,
		StartLoc: info.curLoc,
		Idx:      info.idx,
	}
}

func (jm *JobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	jm.mu.Lock()
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	jm.mu.Unlock()
	log.L().Info("on worker offline ", zap.Any(" worker :", worker.ID()))
	if !exist {
		log.L().Panic("bad worker found", zap.Any("message", worker.ID()))
	}
	if derrors.ErrWorkerFinish.Equal(reason) {
		log.L().Info("worker finished", zap.String("worker-id", worker.ID()), zap.Any("status", worker.Status()), zap.Error(reason))
		return nil
	}

	workerID, err := jm.CreateWorker(lib.CvsTask, getTaskConfig(syncInfo, jm.syncInfo), 10)
	if err != nil {
		log.L().Info("create worker failed ", zap.String(" information :", err.Error()))
	}
	jm.mu.Lock()
	defer jm.mu.Unlock()

	delete(jm.syncFilesInfo, worker.ID())
	jm.syncFilesInfo[workerID] = &workerInfo{
		id:     workerID,
		idx:    syncInfo.idx,
		curLoc: syncInfo.curLoc,
	}
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
	return lib.WorkerStatus{
		Code:     jm.status,
		ExtBytes: []byte(fmt.Sprintf("%d", jm.counter)),
	}
}

func (jm *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

func (jm *JobMaster) listSrcFiles(ctx context.Context) (int, error) {
	conn, err := grpc.Dial(jm.syncInfo.SrcHost, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the host  ", zap.Any("message", jm.syncInfo.SrcHost))
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
