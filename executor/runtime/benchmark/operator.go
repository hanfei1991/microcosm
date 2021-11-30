package benchmark

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type fileWriter struct {
	filePath string
	fd       *os.File
	tid      int32
}

func (f *fileWriter) Prepare() error {
	dir := filepath.Dir(f.filePath)
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o777)
	f.fd = file
	return err
}

func (f *fileWriter) write(_ *runtime.TaskContext, r *runtime.Record) error {
	r.End = time.Now()
	str := []byte(r.String())
	// ctx.stats[f.tid].recordCnt ++
	// ctx.stats[f.tid].totalLag += r.end.Sub(r.start)
	_, err := f.fd.Write(str)
	return err
}

type Closeable interface {
	Close() error
}

type opReceive struct {
	flowID string
	addr   string
	data   chan *runtime.Record
	cache  runtime.Chunk
	conn   Closeable
	errCh  chan error

	running      bool
	binlogClient pb.TestService_FeedBinlogClient
}

func (o *opReceive) NextWantedInputIdx() int { return -1 }

func (o *opReceive) Close() error {
	return o.conn.Close()
}

func (o *opReceive) dial() (client pb.TestServiceClient, err error) {
	// get connection
	log.L().Info("dial to", zap.String("addr", o.addr))
	if test.GlobalTestFlag {
		conn, err := mock.Dial(o.addr)
		o.conn = conn
		if err != nil {
			return nil, errors.New("conn failed")
		}
		client = mock.NewTestClient(conn)
	} else {
		conn, err := grpc.Dial(o.addr, grpc.WithInsecure(), grpc.WithBlock())
		o.conn = conn
		if err != nil {
			return nil, errors.New("conn failed")
		}
		client = pb.NewTestServiceClient(conn)
	}
	return
}

func (o *opReceive) Prepare() error {
	client, err := o.dial()
	if err != nil {
		return errors.New("conn failed")
	}
	// start receiving data
	// TODO: implement recover from a gtid point during failover.
	o.binlogClient, err = client.FeedBinlog(context.Background(), &pb.TestBinlogRequest{Gtid: 0})
	if err != nil {
		return errors.New("conn failed")
	}

	return nil
}

func (o *opReceive) Next(ctx *runtime.TaskContext, _ *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	if !o.running {
		o.running = true
		go func() {
			for {
				record, err := o.binlogClient.Recv()
				if err != nil {
					o.errCh <- err
					log.L().Error("opReceive meet error", zap.Error(err))
					return
				}
				r := &runtime.Record{
					Tid:     record.Tid,
					Payload: record,
				}
				o.data <- r
				ctx.Wake()
			}
		}()
	}
	o.cache = o.cache[:0]
	i := 0
	for ; i < 1024; i++ {
		select {
		case r := <-o.data:
			o.cache = append(o.cache, r)
		case err := <-o.errCh:
			return nil, true, err
		default:
			break
		}
	}
	if i == 0 {
		return nil, true, nil
	}
	return []runtime.Chunk{o.cache}, false, nil
}

type opSyncer struct{}

func (o *opSyncer) Close() error { return nil }

// TODO communicate with master.
func (o *opSyncer) Prepare() error { return nil }

func (o *opSyncer) syncDDL(ctx *runtime.TaskContext) {
	time.Sleep(1 * time.Second)
	ctx.Wake()
}

func (o *opSyncer) Next(ctx *runtime.TaskContext, r *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	record := r.Payload.(*pb.Record)
	if record.Tp == pb.Record_DDL {
		go o.syncDDL(ctx)
		return nil, true, nil
	}
	return []runtime.Chunk{{r}}, false, nil
}

func (o *opSyncer) NextWantedInputIdx() int { return 0 }

type opSink struct {
	writer fileWriter
}

func (o *opSink) Close() error { return nil }

func (o *opSink) Prepare() error {
	return o.writer.Prepare()
}

func (o *opSink) Next(ctx *runtime.TaskContext, r *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	if test.GlobalTestFlag {
		//	log.L().Info("send record", zap.Int32("table", r.Tid), zap.Int32("pk", r.payload.(*pb.Record).Pk))
		ctx.TestCtx.SendRecord(r)
		return nil, false, nil
	}
	return nil, false, o.writer.write(ctx, r)
}

func (o *opSink) NextWantedInputIdx() int { return 0 }
