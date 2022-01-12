package servermaster

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib/fake"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/servermaster/cluster"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/p2p"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Server handles PRC requests for df master.
type Server struct {
	etcd *embed.Etcd

	etcdClient *clientv3.Client
	session    *concurrency.Session
	election   cluster.Election
	leaderCtx  context.Context
	resignFn   context.CancelFunc
	leader     atomic.Value
	members    struct {
		sync.RWMutex
		m []*Member
	}
	leaderClient *client.MasterClient

	messageServer p2p.MessageServer
	messageRouter p2p.MessageRouter

	// sched scheduler
	executorManager *ExecutorManager
	jobManager      *JobManager
	//
	cfg *Config

	initialized atomic.Bool

	// mocked server for test
	mockGrpcServer mock.GrpcServer

	testCtx *test.Context

	fakeMaster *fake.Master
}

// NewServer creates a new master-server.
func NewServer(cfg *Config, ctx *test.Context) (*Server, error) {
	executorNotifier := make(chan model.ExecutorID, 100)
	executorManager := NewExecutorManager(executorNotifier, cfg.KeepAliveTTL, cfg.KeepAliveInterval, ctx)

	urls, err := parseURLs(cfg.MasterAddr)
	if err != nil {
		return nil, err
	}
	masterAddrs := make([]string, 0, len(urls))
	for _, u := range urls {
		masterAddrs = append(masterAddrs, u.Host)
	}
	jobManager := NewJobManager(masterAddrs)

	//messageServer := p2p.NewMessageServer(cfg.)

	server := &Server{
		cfg:             cfg,
		executorManager: executorManager,
		jobManager:      jobManager,
		initialized:     *atomic.NewBool(false),
		testCtx:         ctx,
		leader:          atomic.Value{},
	}
	return server, nil
}

// Heartbeat implements pb interface.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	var (
		resp2 *pb.HeartbeatResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	checkErr := s.apiPreCheck()
	if checkErr != nil {
		return &pb.HeartbeatResponse{Err: checkErr}, nil
	}
	resp, err := s.executorManager.HandleHeartbeat(req)
	if err == nil && resp.Err == nil {
		s.members.RLock()
		defer s.members.RUnlock()
		addrs := make([]string, 0, len(s.members.m))
		for _, member := range s.members.m {
			addrs = append(addrs, strings.Join(member.Addrs, ","))
		}
		resp.Addrs = addrs
		leader, exists := s.checkLeader()
		if exists {
			resp.Leader = strings.Join(leader.Addrs, ",")
		}
	}
	return resp, err
}

// SubmitJob passes request onto "JobManager".
func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	var (
		resp2 *pb.SubmitJobResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.apiPreCheck()
	if err != nil {
		return &pb.SubmitJobResponse{Err: err}, nil
	}
	return s.jobManager.SubmitJob(ctx, req), nil
}

func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	var (
		resp2 *pb.CancelJobResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.apiPreCheck()
	if err != nil {
		return &pb.CancelJobResponse{Err: err}, nil
	}
	return s.jobManager.CancelJob(ctx, req), nil
}

func (s *Server) PauseJob(ctx context.Context, req *pb.PauseJobRequest) (*pb.PauseJobResponse, error) {
	var (
		resp2 *pb.PauseJobResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	err := s.apiPreCheck()
	if err != nil {
		return &pb.PauseJobResponse{Err: err}, nil
	}
	return s.jobManager.PauseJob(ctx, req), nil
}

// RegisterExecutor implements grpc interface, and passes request onto executor manager.
func (s *Server) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest) (*pb.RegisterExecutorResponse, error) {
	var (
		resp2 *pb.RegisterExecutorResponse
		err2  error
	)
	shouldRet := s.rpcForwardIfNeeded(ctx, req, &resp2, &err2)
	if shouldRet {
		return resp2, err2
	}

	ckErr := s.apiPreCheck()
	if ckErr != nil {
		return &pb.RegisterExecutorResponse{Err: ckErr}, nil
	}
	// register executor to scheduler
	// TODO: check leader, if not leader, return notLeader error.
	execInfo, err := s.executorManager.AllocateNewExec(req)
	if err != nil {
		log.L().Logger.Error("add executor failed", zap.Error(err))
		return &pb.RegisterExecutorResponse{
			Err: errors.ToPBError(err),
		}, nil
	}
	return &pb.RegisterExecutorResponse{
		ExecutorId: string(execInfo.ID),
	}, nil
}

// ScheduleTask implements grpc interface. It works as follows
// - receives request from job master
// - queries resource manager to allocate resource and maps tasks to executors
// - returns scheduler response to job master
func (s *Server) ScheduleTask(ctx context.Context, req *pb.TaskSchedulerRequest) (*pb.TaskSchedulerResponse, error) {
	tasks := req.GetTasks()
	success, resp := s.executorManager.Allocate(tasks)
	if !success {
		return nil, errors.ErrClusterResourceNotEnough.GenWithStackByArgs()
	}
	return resp, nil
}

// DeleteExecutor deletes an executor, but have yet implemented.
func (s *Server) DeleteExecutor() {
	// To implement
}

// RegisterMetaStore registers backend metastore to server master,
// but have not implemented yet.
func (s *Server) RegisterMetaStore(
	ctx context.Context, req *pb.RegisterMetaStoreRequest,
) (*pb.RegisterMetaStoreResponse, error) {
	return nil, nil
}

// QueryMetaStore implements gRPC interface
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

func (s *Server) ReportExecutorWorkload(
	ctx context.Context, req *pb.ExecWorkloadRequest,
) (*pb.ExecWorkloadResponse, error) {
	// TODO: pass executor workload to resource manager
	log.L().Debug("receive workload report", zap.String("executor", req.ExecutorId))
	for _, res := range req.GetWorkloads() {
		log.L().Debug("workload", zap.Int32("type", int32(res.GetTp())), zap.Int32("usage", res.GetUsage()))
	}
	return &pb.ExecWorkloadResponse{}, nil
}

func (s *Server) startForTest(ctx context.Context) (err error) {
	// TODO: implement mock-etcd and leader election

	s.mockGrpcServer, err = mock.NewMasterServer(s.cfg.MasterAddr, s)
	if err != nil {
		return err
	}

	s.executorManager.Start(ctx)
	err = s.jobManager.Start(ctx, s.testCtx.GetMetaKV())
	if err != nil {
		return
	}
	s.leader.Store(&Member{Name: s.name(), IsServLeader: true, IsEtcdLeader: true})
	s.initialized.Store(true)
	return
}

// Stop and clean resources.
// TODO: implement stop gracefully.
func (s *Server) Stop() {
	if s.mockGrpcServer != nil {
		s.mockGrpcServer.Stop()
	}
	if s.etcdClient != nil {
		s.etcdClient.Close()
	}
	if s.leaderClient != nil {
		s.leaderClient.Close()
	}
	if s.etcd != nil {
		s.etcd.Close()
	}
}

// Run the master-server.
func (s *Server) Run(ctx context.Context) (err error) {
	if test.GetGlobalTestFlag() {
		return s.startForTest(ctx)
	}

	err = s.startGrpcSrv()
	if err != nil {
		return
	}
	err = s.reset(ctx)
	if err != nil {
		return
	}
	s.initialized.Store(true)

	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return s.leaderLoop(ctx)
	})

	wg.Go(func() error {
		return s.memberLoop(ctx)
	})

	return wg.Wait()
}

func (s *Server) startGrpcSrv() (err error) {
	etcdCfg := etcdutils.GenEmbedEtcdConfigWithLogger(s.cfg.LogLevel)
	// prepare to join an existing etcd cluster.
	err = etcdutils.PrepareJoinEtcd(s.cfg.Etcd, s.cfg.MasterAddr)
	if err != nil {
		return
	}
	log.L().Info("config after join prepared", zap.Stringer("config", s.cfg))

	// generates embed etcd config before any concurrent gRPC calls.
	// potential concurrent gRPC calls:
	//   - workerrpc.NewGRPCClient
	//   - getHTTPAPIHandler
	// no `String` method exists for embed.Config, and can not marshal it to join too.
	// but when starting embed etcd server, the etcd pkg will log the config.
	// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/etcd.go#L299
	etcdCfg, err = etcdutils.GenEmbedEtcdConfig(etcdCfg, s.cfg.MasterAddr, s.cfg.AdvertiseAddr, s.cfg.Etcd)
	if err != nil {
		return
	}

	gRPCSvr := func(gs *grpc.Server) {
		pb.RegisterMasterServer(gs, s)
		// TODO: register msg server
	}

	httpHandlers := map[string]http.Handler{
		"/debug/": getDebugHandler(),
	}

	// generate grpcServer
	s.etcd, err = startEtcd(etcdCfg, gRPCSvr, httpHandlers, etcdStartTimeout)
	if err != nil {
		return
	}
	log.L().Logger.Info("start etcd successfully")

	// start grpc server
	s.etcdClient, err = etcdutil.CreateClient([]string{withHost(s.cfg.MasterAddr)}, nil)
	return
}

// member returns member information of the server
func (s *Server) member() string {
	m := &Member{
		Name:  s.name(),
		Addrs: []string{s.cfg.AdvertiseAddr},
	}
	val, err := m.String()
	if err != nil {
		return s.name()
	}
	return val
}

// name is a shortcut to etcd name
func (s *Server) name() string {
	return s.cfg.Etcd.Name
}

func (s *Server) reset(ctx context.Context) error {
	sess, err := concurrency.NewSession(
		s.etcdClient, concurrency.WithTTL(int(defaultSessionTTL.Seconds())))
	if err != nil {
		return errors.Wrap(errors.ErrMasterNewServer, err)
	}
	_, err = s.etcdClient.Put(ctx, adapter.MasterInfoKey.Encode(s.name()),
		s.cfg.String(), clientv3.WithLease(sess.Lease()))
	if err != nil {
		return errors.Wrap(errors.ErrEtcdAPIError, err)
	}

	s.session = sess
	s.election, err = cluster.NewEtcdElection(ctx, s.etcdClient, sess, cluster.EtcdElectionConfig{
		CreateSessionTimeout: s.cfg.RPCTimeout, // FIXME (zixiong): use a separate timeout here
		TTL:                  s.cfg.KeepAliveTTL,
		Prefix:               adapter.MasterCampaignKey.Path(),
	})
	if err != nil {
		return err
	}
	err = s.updateServerMasterMembers(ctx)
	if err != nil {
		log.L().Warn("failed to update server master members", zap.Error(err))
	}
	return nil
}

func (s *Server) runLeaderService(ctx context.Context) (err error) {
	// rebuild states from existing meta if needed
	err = s.resetExecutor(ctx)
	if err != nil {
		return
	}

	// start background managers
	s.executorManager.Start(ctx)
	err = s.jobManager.Start(ctx, metadata.NewMetaEtcd(s.etcdClient))
	if err != nil {
		return
	}

	s.leader.Store(&Member{
		Name:         s.name(),
		IsServLeader: true,
		IsEtcdLeader: true,
		Addrs:        []string{s.cfg.AdvertiseAddr},
	})
	defer func() {
		s.leader.Store(&Member{})
		s.resign()
	}()

	leaderTicker := time.NewTicker(time.Millisecond * 200)
	defer leaderTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			// ctx is a leaderCtx actually
			return ctx.Err()
		case <-leaderTicker.C:
			if !s.isEtcdLeader() {
				log.L().Info("etcd leader changed, resigns server master leader",
					zap.String("old-leader-name", s.name()))
				return errors.ErrEtcdLeaderChanged.GenWithStackByArgs()
			}
		}
	}
}

func (s *Server) checkLeader() (leader *Member, exist bool) {
	lp := s.leader.Load()
	if lp == nil {
		return
	}
	leader = lp.(*Member)
	exist = leader.Name != ""
	return
}

func (s *Server) isLeaderAndNeedForward(ctx context.Context) (isLeader, needForward bool) {
	leader, exist := s.checkLeader()
	// leader is nil, retry for 3 seconds
	if !exist {
		retry := 10
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for exist {
			if retry == 0 {
				log.L().Error("leader is not found, please retry later")
				return false, false
			}
			select {
			case <-ctx.Done():
				return false, false
			case <-ticker.C:
				retry--
			}
			leader, exist = s.checkLeader()
		}
	}
	isLeader = leader.Name == s.name()
	needForward = s.leaderClient != nil
	return
}

// rpcForwardIfNeeded forwards gRPC if needed.
// arguments with `Pointer` suffix should be pointer to that variable its name indicated
// return `true` means caller should return with variable that `xxPointer` modified.
func (s *Server) rpcForwardIfNeeded(ctx context.Context, req interface{}, respPointer interface{}, errPointer *error) bool {
	pc, _, _, _ := runtime.Caller(1)
	fullMethodName := runtime.FuncForPC(pc).Name()
	methodName := fullMethodName[strings.LastIndexByte(fullMethodName, '.')+1:]

	log.L().Info("", zap.Any("payload", req), zap.String("request", methodName))

	isLeader, needForward := s.isLeaderAndNeedForward(ctx)
	if isLeader {
		return false
	}
	if needForward {
		leader, exist := s.checkLeader()
		if !exist {
			respType := reflect.ValueOf(respPointer).Elem().Type()
			reflect.ValueOf(respPointer).Elem().Set(reflect.Zero(respType))
			*errPointer = errors.ErrMasterRPCNotForward.GenWithStackByArgs()
			return true
		}
		log.L().Info("will forward rpc request", zap.String("from", s.name()),
			zap.String("to", leader.Name), zap.String("request", methodName))

		params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
		results := reflect.ValueOf(s.leaderClient.GetLeaderClient()).MethodByName(methodName).Call(params)
		// result's inner types should be (*pb.XXResponse, error), which is same as s.leaderClient.XXRPCMethod
		reflect.ValueOf(respPointer).Elem().Set(results[0])
		errInterface := results[1].Interface()
		// nil can't pass type conversion, so we handle it separately
		if errInterface == nil {
			*errPointer = nil
		} else {
			*errPointer = errInterface.(error)
		}
		return true
	}
	respType := reflect.ValueOf(respPointer).Elem().Type()
	reflect.ValueOf(respPointer).Elem().Set(reflect.Zero(respType))
	*errPointer = errors.ErrMasterRPCNotForward.GenWithStackByArgs()
	return true
}

// apiPreCheck checks whether server master(leader) is ready to serve
func (s *Server) apiPreCheck() *pb.Error {
	if !s.initialized.Load() {
		return &pb.Error{
			Code: pb.ErrorCode_MasterNotReady,
		}
	}
	return nil
}

func withHost(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// do nothing
		return addr
	}
	if len(host) == 0 {
		return fmt.Sprintf("127.0.0.1:%s", port)
	}

	return addr
}

func (s *Server) memberLoop(ctx context.Context) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.updateServerMasterMembers(ctx); err != nil {
				log.L().Warn("update server master members failed", zap.Error(err))
			}
		}
	}
}
