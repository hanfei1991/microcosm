package cvs

import (
	"context"
	"fmt"
	"strconv"
	"sync"
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
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

var _ lib.JobMasterImpl = &JobMaster{}

type Config struct {
	SrcHost string `toml:"srcHost" json:"srcHost"`
	SrcDir  string `toml:"srcDir" json:"srcDir"`
	DstHost string `toml:"dstHost" json:"dstHost"`
	DstDir  string `toml:"dstDir" json:"dstDir"`
}

type workerInfo struct {
	idx    int
	curLoc int64
	handle lib.WorkerHandle
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
	syncFilesInfoMu   sync.Mutex
	syncFilesInfo     map[lib.WorkerID]*workerInfo
	counter           int64
	workerID          lib.WorkerID
	filesNum          int
	statusRateLimiter *rate.Limiter
	statusCode        struct {
		sync.RWMutex
		code lib.WorkerStatusCode
	}
	ctx     context.Context
	clocker clock.Clock
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
	jm.ctx = ctx.Context
	jm.clocker = clock.New()
	log.L().Info("new cvs jobmaster ", zap.Any("id :", jm.workerID))
	return jm
}

func (jm *JobMaster) InitImpl(ctx context.Context) (err error) {
	if jm.syncInfo.DstHost == jm.syncInfo.SrcHost && jm.syncInfo.SrcDir == jm.syncInfo.DstDir {
		return &errorInfo{info: "bad configure file ,make sure the source address is not the same as the destination"}
	}
	log.L().Info("initializing the cvs jobmaster  ", zap.Any("id :", jm.workerID))
	jm.setStatusCode(lib.WorkerStatusInit)
	jm.filesNum, err = jm.listSrcFiles(ctx)
	if err != nil {
		return err
	}
	if jm.filesNum == 0 {
		log.L().Info("no file found under the folder ", zap.Any("message", jm.syncInfo.DstDir))
	}
	log.L().Info(" cvs jobmaster list file success", zap.Any("id :", jm.workerID), zap.Any(" file number :", jm.filesNum))
	// todo: store the jobmaster information into the metastore
	jm.syncFilesInfoMu.Lock()
	defer jm.syncFilesInfoMu.Unlock()
	for idx := 0; idx < jm.filesNum; idx++ {
		conf := cvsTask.Config{SrcHost: jm.syncInfo.SrcHost, Idx: idx, DstHost: jm.syncInfo.DstHost, DstDir: jm.syncInfo.DstDir, StartLoc: "0"}
		workerID, err := jm.CreateWorker(lib.CvsTask, conf, 10 /* TODO add cost */)
		if err != nil {
			// todo : handle the error case, should recover for failing to create worker
			return err
		}
		jm.syncFilesInfo[workerID] = &workerInfo{idx: idx, curLoc: 0, handle: nil}
	}
	return nil
}

func (jm *JobMaster) Tick(ctx context.Context) error {
	filesNum := 0
	jm.counter = 0
	if jm.getStatusCode() == lib.WorkerStatusInit {
		err := jm.UpdateJobStatus(ctx, jm.Status())
		if err != nil {
			log.L().Warn("update job status failed", zap.Any("id", jm.workerID), zap.Error(err))
		} else {
			jm.setStatusCode(lib.WorkerStatusNormal)
		}
	}
	jm.syncFilesInfoMu.Lock()
	defer jm.syncFilesInfoMu.Unlock()
	for _, worker := range jm.syncFilesInfo {
		if worker.handle == nil {
			continue
		}
		status := worker.handle.Status()
		switch status.Code {
		case lib.WorkerStatusNormal, lib.WorkerStatusFinished, lib.WorkerStatusStopped:
			num, err := strconv.ParseInt(string(status.ExtBytes), 10, 64)
			if err != nil {
				return err
			}
			worker.curLoc = num
			jm.counter += num
			log.L().Debug("cvs job tmp num ", zap.Any("id", worker.handle.ID()), zap.Int64("counter", num), zap.Any("status", status.Code))
			// todo : store the sync progress into the meta store for each file
			if status.Code == lib.WorkerStatusFinished {
				filesNum++
			}
		case lib.WorkerStatusError:
			log.L().Error("sync file failed ", zap.Any("idx", worker.idx))
		default:
			log.L().Info("worker status abnormal", zap.Any("status", status))
		}
	}
	if jm.statusRateLimiter.Allow() {
		log.L().Info("cvs job master status", zap.Any("id", jm.workerID), zap.Int64("counter", jm.counter), zap.Any("status", jm.getStatusCode()))
	}
	if jm.getStatusCode() == lib.WorkerStatusStopped {
		log.L().Info("cvs job master stopped")
		return jm.BaseJobMaster.Exit(ctx, jm.Status(), nil)
	}
	if filesNum == jm.filesNum {
		jm.setStatusCode(lib.WorkerStatusFinished)
		log.L().Info("cvs job master finished")
		return jm.BaseJobMaster.Exit(ctx, jm.Status(), nil)
	}
	return nil
}

func (jm *JobMaster) OnMasterRecovered(ctx context.Context) error {
	return nil
}

func (jm *JobMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	return nil
}

func (jm *JobMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	// todo : add the worker information to the sync files map
	jm.syncFilesInfoMu.Lock()
	defer jm.syncFilesInfoMu.Unlock()
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	if !exist {
		log.L().Panic("phantom worker found", zap.Any("id", worker.ID()))
	} else {
		log.L().Info("worker online ", zap.Any("id", worker.ID()))
	}
	syncInfo.handle = worker
	return nil
}

func (jm *JobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	jm.syncFilesInfoMu.Lock()
	defer jm.syncFilesInfoMu.Unlock()
	syncInfo, exist := jm.syncFilesInfo[worker.ID()]
	log.L().Info("on worker offline ", zap.Any(" worker :", worker.ID()))
	if !exist {
		log.L().Panic("bad worker found", zap.Any("message", worker.ID()))
	}
	// Force to set worker handle, in case of worker finishes before OnWorkerOnline
	// is fired and worker handle is missing
	syncInfo.handle = worker
	log.L().Info("worker finished", zap.String("worker-id", worker.ID()), zap.Any("status", worker.Status()), zap.Error(reason))
	// TODO failover
	return nil
	//var err error
	//dstDir := jm.syncInfo.DstDir + "/" + syncInfo.file
	//srcDir := jm.syncInfo.SrcDir + "/" + syncInfo.file
	//conf := cvsTask.Config{SrcHost: jm.syncInfo.SrcHost, SrcDir: srcDir, DstHost: jm.syncInfo.DstHost, DstDir: dstDir, StartLoc: syncInfo.curLoc}
	//workerID, err := jm.CreateWorker(lib.CvsTask, conf, 10)
	//if err != nil {
	//	log.L().Info("create worker failed ", zap.String(" information :", err.Error()))
	//}
	//delete(jm.syncFilesInfo, worker.ID())
	//// todo : if the worker id is empty ,the sync file will be lost.
	//jm.syncFilesInfo[workerID] = &workerInfo{file: syncInfo.file, curLoc: syncInfo.curLoc, handle: nil}
}

func (jm *JobMaster) OnWorkerStatusUpdated(worker lib.WorkerHandle, newStatus *lib.WorkerStatus) error {
	return nil
}

func (jm *JobMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message p2p.MessageValue) error {
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

func (jm *JobMaster) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	return nil
}

func (jm *JobMaster) OnJobManagerFailover(reason lib.MasterFailoverReason) error {
	log.L().Info("cvs jobmaster: OnJobManagerFailover", zap.Any("reason", reason))
	return nil
}

func (jm *JobMaster) OnJobManagerMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("cvs jobmaster: OnJobManagerMessage", zap.Any("message", message))
	switch msg := message.(type) {
	case *lib.StatusChangeRequest:
		switch msg.ExpectState {
		case lib.WorkerStatusStopped:
			jm.setStatusCode(lib.WorkerStatusStopped)
			jm.syncFilesInfoMu.Lock()
			defer jm.syncFilesInfoMu.Unlock()
			for _, worker := range jm.syncFilesInfo {
				if worker.handle == nil {
					continue
				}
				wTopic := lib.WorkerStatusChangeRequestTopic(jm.BaseJobMaster.ID(), worker.handle.ID())
				wMessage := &lib.StatusChangeRequest{
					SendTime:     jm.clocker.Mono(),
					FromMasterID: jm.BaseJobMaster.ID(),
					Epoch:        jm.BaseJobMaster.CurrentEpoch(),
					ExpectState:  lib.WorkerStatusStopped,
				}
				ctx, cancel := context.WithTimeout(jm.ctx, time.Second*2)
				if err := worker.handle.SendMessage(ctx, wTopic, wMessage, false /*nonblocking*/); err != nil {
					cancel()
					return err
				}
				log.L().Info("sent message to worker", zap.String("topic", topic), zap.Any("message", wMessage))
				cancel()
			}
		default:
			log.L().Info("FakeMaster: ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.L().Info("unsupported message", zap.Any("message", message))
	}

	return nil
}

func (jm *JobMaster) Status() lib.WorkerStatus {
	return lib.WorkerStatus{
		Code:     jm.getStatusCode(),
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

func (jm *JobMaster) setStatusCode(code lib.WorkerStatusCode) {
	jm.statusCode.Lock()
	defer jm.statusCode.Unlock()
	jm.statusCode.code = code
}

func (jm *JobMaster) getStatusCode() lib.WorkerStatusCode {
	jm.statusCode.RLock()
	defer jm.statusCode.RUnlock()
	return jm.statusCode.code
}
