package producer

import (
	"fmt"
	"io"
	"sync"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test/mock"
)

//const (
//	port = ":50051"
//)

//type server struct {
//	pb.TmpServiceServer
//}
//
//func (s *server) EventFeed(in *pb.Request, server pb.TmpService_EventFeedServer) error {
//	tid := in.MaxTid
//	log.Printf("here comes new req %d", tid)
//	i := int32(0)
//	for {
//		tm := time.Now().Format(time.RFC3339Nano)
//		err := server.Send(&pb.Record{
//			StartTs: []byte(tm),
//			Tid:     i,
//			Payload: []byte(tm),
//		})
//		if err != nil {
//			log.Printf("meet error %v", err)
//			return err
//		}
//		i = (i + 1) % tid
//	}
//}
//
type record struct {
	pk        int32
	schemaVer int32
	tp        pb.Record_RecordType
	tid       int32
	gtid      int32
}

func (r *record) toPB() *pb.Record {
	return &pb.Record{
		Tp: r.tp,
		SchemaVer: r.schemaVer,
		Tid: r.tid,
		Gtid: r.gtid,
		Pk: r.pk,
	}	
}

type tableProducer struct {
	tid  int32
	pk   int32
	schemaVer int32
	binlogChan []chan record 
}

func (t *tableProducer) run() {
	binlogID := 0
	for {
		t.pk ++
		if t.pk % 1000 == 0 {
			t.schemaVer ++
			ddlLock.Lock()
			for _, ch := range t.binlogChan {
				ch <- record {
					tp: pb.Record_DDL,
					tid : t.tid,
					schemaVer: t.schemaVer,
				}
			}
			ddlLock.Unlock()
		}
		ch := t.binlogChan[binlogID]
		ch <- record {
			tp : pb.Record_Data,
			tid : t.tid,
			pk: t.pk,
		}
	}
}

var ddlLock = new(sync.Mutex)

type BinlogTestServer struct {
	binlogChan chan record
	wal     []record
	addr    string
}


func (b * BinlogTestServer) FeedBinlog(req *pb.TestBinlogRequest, server pb.TestService_FeedBinlogServer) error {
	id := int(req.Gtid)
	if id > len(b.wal) {
		return server.Send(&pb.Record{
			Err: &pb.Error{Message: fmt.Sprintf("invalid gtid %d", id)},
		})
	}
	for id < len(b.wal) {
		err := server.Send(b.wal[id].toPB())
		id ++
		if err != nil {
			return err
		}
	}
	for record := range b.binlogChan {
		record.gtid = int32(len(b.wal))
		b.wal = append(b.wal, record)
		err := server.Send(b.wal[id].toPB())
		if err != nil {
			return err
		}
	}
	return io.EOF
}

func StartProducerForTest(addrs []string, tableNum int32) ([]mock.GrpcServer, error) {
	channels := make([]chan record, 0, len(addrs))
	servers := make([]mock.GrpcServer, 0, len(addrs))
	for _, addr := range addrs {
		server := &BinlogTestServer{
			binlogChan: make(chan record, 1024),
			addr: addr,
		}
		channels = append(channels, server.binlogChan)

		grpcServer, err := mock.NewTestServer(addr, server)
		if err != nil {
			return nil, err
		}
		servers = append(servers, grpcServer)
	}
	for tid := int32(0); tid < tableNum; tid ++ {
		producer := &tableProducer{
			tid: tid,
			binlogChan: channels,
		}
		go producer.run()
	}
	return servers, nil
}

//func main() {
//	lis, err := net.Listen("tcp", port)
//	if err != nil {
//		log.Fatalf("fail to listen: %v", err)
//	}
//	s := grpc.NewServer()
//	pb.RegisterTmpServiceServer(s, &server{})
//	log.Printf("server listening at %v", lis.Addr())
//	if err := s.Serve(lis); err != nil {
//		log.Fatalf("failed to serve: %v", err)
//	}
//}
