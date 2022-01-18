package cvstask

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/log"
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

type cvsTask struct {
	*lib.BaseWorker
	srcAddr strPair
	dstAddr strPair
	counter int64
	buffer  chan strPair
}

func NewCvsTask(src strPair, dst strPair) lib.WorkerImpl {
	task := &cvsTask{}
	task.Impl = task
	task.srcAddr = src
	task.dstAddr = dst
	task.counter = 0
	task.buffer = make(chan strPair, BUFFERSIZE)
	return task

}

func (task *cvsTask) InitImpl(ctx context.Context) error {
	go func() {
		err := task.Receive(ctx)
		if err != nil {
			fmt.Printf("error happened when receive data from upstream %v", err)
		}
	}()
	go func() {
		err := task.Send(ctx)
		if err != nil {
			fmt.Printf("error happened when send  data to downstream  %v", err)
		}
	}()
	return nil
}

// Tick is called on a fixed interval.
func (task *cvsTask) Tick(ctx context.Context) error {
	return nil
}

// Status returns a short worker status to be periodically sent to the master.
func (task *cvsTask) Status() (lib.WorkerStatus, error) {
	return lib.WorkerStatus{Code: lib.WorkerStatusNormal, ErrorMessage: "", Ext: task.counter}, nil
}

// Workload returns the current workload of the worker.
func (task *cvsTask) Workload() (model.RescUnit, error) {
	return 1, nil
}

// OnMasterFailover is called when the master is failed over.
func (task *cvsTask) OnMasterFailover(reason lib.MasterFailoverReason) error {
	return nil
}

// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
func (task *cvsTask) CloseImpl() {
	return
}

func (c *cvsTask) Receive(ctx context.Context) error {
	conn, err := grpc.Dial(c.srcAddr.firstStr, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the source address ", zap.Any("message", c.srcAddr.firstStr))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	reader, err := client.ReadLines(ctx, &pb.ReadLinesRequest{FileName: c.srcAddr.secondStr})
	if err != nil {
		log.L().Info("read data from file failed ", zap.Any("message", c.srcAddr.secondStr))
		return err
	}
	for {
		linestr, err := reader.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.L().Info("read data failed")
		}
		strs := strings.Split(linestr.Linestr, ",")
		if len(strs) < 2 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case c.buffer <- strPair{firstStr: strs[0], secondStr: strs[1]}:
		}
	}
	return nil
}

func (c *cvsTask) Send(ctx context.Context) error {
	conn, err := grpc.Dial(c.dstAddr.firstStr, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the destination address ", zap.Any("message", c.srcAddr.firstStr))
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
		case kv := <-c.buffer:
			err := writer.Send(&pb.WriteLinesRequest{FileName: c.dstAddr.secondStr, Key: kv.firstStr, Value: kv.secondStr})
			c.counter++
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
