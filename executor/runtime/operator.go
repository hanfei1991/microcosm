package runtime

import (
	"context"
	"errors"
	"os"
	"time"

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

func (f *fileWriter) prepare() error {
	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o777)
	f.fd = file
	return err
}

func (f *fileWriter) write(_ *taskContext, r *Record) error {
	r.end = time.Now()
	str := []byte(r.toString())
	// ctx.stats[f.tid].recordCnt ++
	// ctx.stats[f.tid].totalLag += r.end.Sub(r.start)
	_, err := f.fd.Write(str)
	return err
}

type tableStats struct {
	totalLag  time.Duration
	recordCnt int
}

type operator interface {
	next(ctx *taskContext, r *Record, idx int) ([]Chunk, bool, error)
	nextWantedInputIdx() int
	prepare() error
	close() error
}

type closeable interface {
	Close() error
}

type opReceive struct {
	addr     string
	tableCnt int32
	data     chan *Record
	cache    Chunk 
	conn     closeable
}

func (o *opReceive) nextWantedInputIdx() int {return 0}

func (o *opReceive) close() error {
	return o.conn.Close()
}

func (o *opReceive) dial() (client pb.TestServiceClient, err error) {
	// get connection
	log.L().Logger.Info("dial to", zap.String("addr", o.addr))
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

func (o *opReceive) prepare() error {
	client, err := o.dial()
	// start receiving data
	// TODO: implement recover from a gtid point during failover.
	stream, err := client.FeedBinlog(context.Background(), &pb.TestBinlogRequest{Gtid: 0})
	if err != nil {
		return errors.New("conn failed")
	}

	go func() {
		for {
			record, err := stream.Recv()
			if err != nil {
				panic(err)
			}
			r := &Record{
				tid:     record.Tid,
			}
			o.data <- r
		}
	}()
	return nil
}

func (o *opReceive) next(ctx *taskContext, _ *Record, _ int) ([]Chunk, bool, error) {
	o.cache = o.cache[:0]
	i := 0
	for ; i < 1024; i++ {
		select {
		case r := <-o.data:
			o.cache = append(o.cache, r)
		default:
			break
		}
	}
	if i == 0 {
		return nil, false, nil
	}
	return []Chunk{o.cache}, false, nil
}

type opSyncer struct{}

func (o *opSyncer) close() error { return nil }

// TODO communicate with master.
func (o *opSyncer) prepare() error { return nil }

func (o *opSyncer) syncDDL(ctx *taskContext) {
	time.Sleep(1*time.Second)
	ctx.wake()
}

func (o *opSyncer) next(ctx *taskContext, r *Record, _ int) ([]Chunk, bool, error) {
		record := r.payload.(*pb.Record)
		if record.Tp == pb.Record_DDL {
			go o.syncDDL(ctx)
			return nil, true, nil
		}	
	return []Chunk{{r}}, false, nil
}

func (o *opSyncer) nextWantedInputIdx() int {return 0}

type opSink struct {
	writer fileWriter
}

func (o *opSink) close() error {return nil}

func (o *opSink) prepare() error {
	return o.writer.prepare()
}

func (o *opSink) next(ctx *taskContext, r *Record, _ int) ([]Chunk, bool, error) {
	return nil, false, o.writer.write(ctx, r)
}

func (o *opSink) nextWantedInputIdx() int {return 0}
