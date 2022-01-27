package cvstask

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	BUFFERSIZE = 1024
)

type strPair struct {
	firstStr  string
	secondStr string
}

type Config struct {
	SrcHost  string `json:"SrcHost"`
	SrcDir   string `json:"SrcDir"`
	DstHost  string `json:"DstHost"`
	DstDir   string `json:"DstDir"`
	StartLoc int64  `json:"StartLoc"`
}

type cvsTask struct {
	*lib.BaseWorker
	srcHost  string
	srcDir   string
	dstHost  string
	dstDir   string
	counter  int64
	index    int64
	status   lib.WorkerStatusCode
	cancelFn func()
	buffer   chan strPair
}

func init() {
	constructor := func(ctx *dcontext.Context, id lib.WorkerID, masterID lib.MasterID, config lib.WorkerConfig) lib.Worker {
		return NewCvsTask(ctx, id, masterID, config)
	}
	factory := registry.NewSimpleWorkerFactory(constructor, &Config{})
	registry.NewRegistry().MustRegisterWorkerType(lib.CvsTask, factory)
}

func NewCvsTask(ctx *dcontext.Context, _workerID lib.WorkerID, masterID lib.MasterID, conf lib.WorkerConfig) *cvsTask {
	cfg := conf.(*Config)
	task := &cvsTask{
		srcHost: cfg.SrcHost,
		srcDir:  cfg.SrcDir,
		dstHost: cfg.DstHost,
		dstDir:  cfg.DstDir,
		index:   cfg.StartLoc,
		buffer:  make(chan strPair, BUFFERSIZE),
	}
	deps := ctx.Dependencies
	base := lib.NewBaseWorker(
		task,
		deps.MessageHandlerManager,
		deps.MessageRouter,
		deps.MetaKVClient,
		_workerID,
		masterID,
	)
	base.Impl = task
	return task
}

func (task *cvsTask) InitImpl(ctx context.Context) error {
	ctx, task.cancelFn = context.WithCancel(ctx)
	go func() {
		err := task.Receive(ctx)
		if err != nil {
			log.L().Info("error happened when reading data from the upstream ", zap.Any("message", err.Error()))
			task.status = lib.WorkerStatusError
		}
	}()
	go func() {
		err := task.Send(ctx)
		if err != nil {
			log.L().Info("error happened when writing data to the downstream ", zap.Any("message", err.Error()))
			task.status = lib.WorkerStatusError
		}
	}()
	return nil
}

// Tick is called on a fixed interval.
func (task *cvsTask) Tick(ctx context.Context) error {
	return nil
}

// Status returns a short worker status to be periodically sent to the master.
func (task *cvsTask) Status() lib.WorkerStatus {
	return lib.WorkerStatus{Code: lib.WorkerStatusNormal, ErrorMessage: "", Ext: task.counter}
}

// Workload returns the current workload of the worker.
func (task *cvsTask) Workload() model.RescUnit {
	return 1
}

// OnMasterFailover is called when the master is failed over.
func (task *cvsTask) OnMasterFailover(reason lib.MasterFailoverReason) error {
	return nil
}

// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
func (task *cvsTask) CloseImpl(ctx context.Context) error {
	task.cancelFn()
	return nil
}

func (task *cvsTask) Receive(ctx context.Context) error {
	conn, err := grpc.Dial(task.srcHost, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the source address ", zap.Any("message", task.srcHost))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	reader, err := client.ReadLines(ctx, &pb.ReadLinesRequest{FileName: task.srcDir, LineNo: task.index})
	if err != nil {
		log.L().Info("read data from file failed ", zap.Any("message", task.srcDir))
		return err
	}
	for {
		linestr, err := reader.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.L().Info("read data failed")
			continue
		}
		strs := strings.Split(linestr.Linestr, ",")
		if len(strs) < 2 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case task.buffer <- strPair{firstStr: strs[0], secondStr: strs[1]}:
		}
	}
	return nil
}

func (task *cvsTask) Send(ctx context.Context) error {
	conn, err := grpc.Dial(task.dstHost, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the destination address ", zap.Any("message", task.dstHost))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	writer, err := client.WriteLines(ctx)
	if err != nil {
		log.L().Info("call write data rpc failed ")
		return err
	}
	timeout := time.NewTimer(time.Second * 100)
	for {
		select {
		case <-ctx.Done():
			return nil
		case kv := <-task.buffer:
			err := writer.Send(&pb.WriteLinesRequest{FileName: task.dstDir, Key: kv.firstStr, Value: kv.secondStr})
			task.counter++
			if err != nil {
				log.L().Info("call write data rpc failed ")
			}
		case <-timeout.C:
			break
		default:
			time.Sleep(time.Second)
		}
	}
}
