package metastore

import (
	"context"
	"fmt"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// time waiting for etcd to be started.
	etcdStartTimeout = time.Minute
)

// Server manages multiple metastore, it handles RPC requests from metastore user,
// and provides abilities including:
// - A service discovery metastore management, it is am embed etcd indeed.
// - A metastore for dataflow engine system
// - A metastore for applications running on dataflow engine
type Server struct {
	etcd *embed.Etcd
	cfg  *Config
}

// NewServer creates a new metastore server.
func NewServer(cfg *Config) (*Server, error) {
	server := &Server{
		cfg: cfg,
	}
	err := cfg.adjust()
	if err != nil {
		return nil, err
	}
	return server, nil
}

// Start the metastore server.
func (s *Server) Start(ctx context.Context) (err error) {
	etcdCfg := etcdutils.GenEmbedEtcdConfigWithLogger(s.cfg.LogLevel)
	// TODO: prepare to join an existing etcd cluster.
	//err = prepareJoinEtcd(s.cfg)
	//if err != nil {
	//	return
	//}
	log.L().Info("config after join prepared", zap.Stringer("config", s.cfg))

	// generates embed etcd config before any concurrent gRPC calls.
	// potential concurrent gRPC calls:
	//   - workerrpc.NewGRPCClient
	//   - getHTTPAPIHandler
	// no `String` method exists for embed.Config, and can not marshal it to join too.
	// but when starting embed etcd server, the etcd pkg will log the config.
	// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/etcd.go#L299
	etcdCfg, err = etcdutils.GenEmbedEtcdConfig(etcdCfg, s.cfg.Addr, s.cfg.AdvertiseAddr, s.cfg.Etcd)
	if err != nil {
		return
	}

	gRPCSvr := func(gs *grpc.Server) {
		pb.RegisterMetaStoreServer(gs, s)
	}

	// generate grpcServer
	s.etcd, err = etcdutils.StartEtcd(etcdCfg, gRPCSvr, nil, etcdStartTimeout)
	if err != nil {
		return
	}

	log.L().Logger.Info("start etcd successfully")
	return nil
}

func (s *Server) RegisterMetaStore(
	ctx context.Context, req *pb.RegisterMetaStoreRequest,
) (*pb.RegisterMetaStoreResponse, error) {
	return nil, nil
}

func (s *Server) QueryMetaStore(
	ctx context.Context, req *pb.QueryMetaStoreRequest,
) (*pb.QueryMetaStoreResponse, error) {
	switch req.Tp {
	case pb.StoreType_ServiceDiscovery:
		return &pb.QueryMetaStoreResponse{
			Address: s.cfg.AdvertiseAddr,
		}, nil
	case pb.StoreType_SystemMetaStore:
		// TODO: independent system metastore
		return &pb.QueryMetaStoreResponse{
			Address: s.cfg.AdvertiseAddr,
		}, nil
	default:
		return &pb.QueryMetaStoreResponse{
			Err: &pb.Error{
				Code:    pb.ErrorCode_InvalidMetaStoreType,
				Message: fmt.Sprintf("store type: %s", req.Tp),
			},
		}, nil
	}
}
