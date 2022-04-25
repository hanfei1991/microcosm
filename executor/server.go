package executor

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/tcpserver"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/worker"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/registry"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/config"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/broker"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
	extkv "github.com/hanfei1991/microcosm/pkg/meta/extension"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
	"github.com/hanfei1991/microcosm/pkg/serverutils"
	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
)

type Server struct {
	cfg     *Config
	testCtx *test.Context

	tcpServer      tcpserver.TCPServer
	grpcSrv        *grpc.Server
	masterClient   client.MasterClient
	resourceClient *rpcutil.FailoverRPCClients[pb.ResourceManagerClient]
	cliUpdateCh    chan cliUpdateInfo
	workerRtm      *worker.TaskRunner
	msgServer      *p2p.MessageRPCService
	info           *model.NodeInfo

	lastHearbeatTime time.Time

	mockSrv mock.GrpcServer

	// etcdCli connects to server master embed etcd, it should be used in service
	// discovery only.
	etcdCli *clientv3.Client
	// framework metastore prefix kvclient
	metaKVClient metaclient.KVClient
	// user metastore raw kvclient(reuse for all workers)
	userRawKVClient extkv.KVClientEx
	p2pMsgRouter    p2pImpl.MessageRouter
	discoveryKeeper *serverutils.DiscoveryKeepaliver
	resourceBroker  broker.Broker
}

func NewServer(cfg *Config, ctx *test.Context) *Server {
	s := Server{
		cfg:         cfg,
		testCtx:     ctx,
		cliUpdateCh: make(chan cliUpdateInfo),
	}
	return &s
}

// SubmitBatchTasks implements the pb interface.
func (s *Server) SubmitBatchTasks(ctx context.Context, req *pb.SubmitBatchTasksRequest) (*pb.SubmitBatchTasksResponse, error) {
	// TODO modify executor.proto and remove this method.
	panic("implement me")
}

// CancelBatchTasks implements pb interface.
func (s *Server) CancelBatchTasks(ctx context.Context, req *pb.CancelBatchTasksRequest) (*pb.CancelBatchTasksResponse, error) {
	// TODO modify executor.proto and remove this method.
	panic("implement me")
}

// PauseBatchTasks implements pb interface.
func (s *Server) PauseBatchTasks(ctx context.Context, req *pb.PauseBatchTasksRequest) (*pb.PauseBatchTasksResponse, error) {
	// TODO modify executor.proto and remove this method.
	panic("implement me")
}

// ResumeBatchTasks implements pb interface.
func (s *Server) ResumeBatchTasks(ctx context.Context, req *pb.PauseBatchTasksRequest) (*pb.PauseBatchTasksResponse, error) {
	// TODO modify executor.proto and remove this method.
	panic("implement me")
}

func (s *Server) buildDeps() (*deps.Deps, error) {
	deps := deps.NewDeps()
	err := deps.Provide(func() p2p.MessageHandlerManager {
		return s.msgServer.MakeHandlerManager()
	})
	if err != nil {
		return nil, err
	}

	err = deps.Provide(func() p2p.MessageSender {
		return p2p.NewMessageSender(s.p2pMsgRouter)
	})
	if err != nil {
		return nil, err
	}

	err = deps.Provide(func() metaclient.KVClient {
		return s.metaKVClient
	})
	if err != nil {
		return nil, err
	}

	err = deps.Provide(func() extkv.KVClientEx {
		return s.userRawKVClient
	})
	if err != nil {
		return nil, err
	}

	err = deps.Provide(func() client.ClientsManager {
		return client.NewClientManager()
	})
	if err != nil {
		return nil, err
	}

	err = deps.Provide(func() client.MasterClient {
		return s.masterClient
	})
	if err != nil {
		return nil, err
	}

	err = deps.Provide(func() broker.Broker {
		return s.resourceBroker
	})
	if err != nil {
		return nil, err
	}

	return deps, nil
}

func (s *Server) DispatchTask(ctx context.Context, req *pb.DispatchTaskRequest) (*pb.DispatchTaskResponse, error) {
	log.L().Info("dispatch task", zap.String("req", req.String()))

	// TODO better dependency management
	dctx := dcontext.Background()
	dctx.Dependencies = dcontext.RuntimeDependencies{
		MessageHandlerManager: s.msgServer.MakeHandlerManager(),
		MessageRouter:         p2p.NewMessageSender(s.p2pMsgRouter),
		MetaKVClient:          s.metaKVClient,
		UserRawKVClient:       s.userRawKVClient,
		ExecutorClientManager: client.NewClientManager(),
		ServerMasterClient:    s.masterClient,
	}

	dp, err := s.buildDeps()
	if err != nil {
		return nil, err
	}
	dctx = dctx.WithDeps(dp)
	dctx.Environ.NodeID = p2p.NodeID(s.info.ID)
	dctx.Environ.Addr = s.info.Addr

	masterMeta := &libModel.MasterMetaKVData{
		// GetWorkerId here returns id of current unit
		ID:     req.GetWorkerId(),
		Tp:     libModel.WorkerType(req.GetTaskTypeId()),
		Config: req.GetTaskConfig(),
	}
	metaBytes, err := masterMeta.Marshal()
	if err != nil {
		return nil, err
	}
	dctx.Environ.MasterMetaBytes = metaBytes

	newWorker, err := registry.GlobalWorkerRegistry().CreateWorker(
		dctx,
		libModel.WorkerType(req.GetTaskTypeId()),
		req.GetWorkerId(),
		req.GetMasterId(),
		req.GetTaskConfig())
	if err != nil {
		log.L().Error("Failed to create worker", zap.Error(err))
		// TODO better error handling
		return nil, err
	}

	if err := s.workerRtm.AddTask(newWorker); err != nil {
		errCode := pb.DispatchTaskErrorCode_Other
		if errors.ErrRuntimeReachedCapacity.Equal(err) || errors.ErrRuntimeIncomingQueueFull.Equal(err) {
			errCode = pb.DispatchTaskErrorCode_NoResource
		}

		return &pb.DispatchTaskResponse{
			ErrorCode:    errCode,
			ErrorMessage: err.Error(),
			WorkerId:     req.GetWorkerId(),
		}, nil
	}

	return &pb.DispatchTaskResponse{
		ErrorCode: pb.DispatchTaskErrorCode_OK,
		WorkerId:  req.GetWorkerId(),
	}, nil
}

func (s *Server) Stop() {
	if s.grpcSrv != nil {
		s.grpcSrv.Stop()
	}

	if s.tcpServer != nil {
		err := s.tcpServer.Close()
		if err != nil {
			log.L().Error("close tcp server", zap.Error(err))
		}
	}

	if s.etcdCli != nil {
		// clear executor info in metastore to accelerate service discovery. If
		// not delete actively, the session will be timeout after TTL.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := s.etcdCli.Delete(ctx, s.info.EtcdKey())
		if err != nil {
			log.L().Warn("failed to delete executor info", zap.Error(err))
		}
	}

	if s.metaKVClient != nil {
		err := s.metaKVClient.Close()
		if err != nil {
			log.L().Warn("failed to close connection to framework metastore", zap.Error(err))
		}
	}

	if s.userRawKVClient != nil {
		err := s.userRawKVClient.Close()
		if err != nil {
			log.L().Warn("failed to close connection to user metastore", zap.Error(err))
		}
	}

	if s.mockSrv != nil {
		s.mockSrv.Stop()
	}
}

func (s *Server) startForTest(ctx context.Context) (err error) {
	s.mockSrv, err = mock.NewExecutorServer(s.cfg.WorkerAddr, s)
	if err != nil {
		return err
	}

	err = s.initClients(ctx)
	if err != nil {
		return err
	}
	err = s.selfRegister(ctx)
	if err != nil {
		return err
	}
	go func() {
		err := s.keepHeartbeat(ctx)
		log.L().Info("heartbeat quits", zap.Error(err))
	}()
	return nil
}

func (s *Server) startMsgService(ctx context.Context, wg *errgroup.Group) (err error) {
	s.msgServer, err = p2p.NewDependentMessageRPCService(string(s.info.ID), nil, s.grpcSrv)
	if err != nil {
		return err
	}
	wg.Go(func() error {
		// TODO refactor this
		return s.msgServer.Serve(ctx, nil)
	})
	return nil
}

const (
	// TODO since we introduced queuing in the TaskRunner, it is no longer
	// easy to implement the capacity. Think of a better solution later.
	// defaultRuntimeCapacity      = 65536
	defaultRuntimeIncomingQueueLen = 256
	defaultRuntimeInitConcurrency  = 256
)

func (s *Server) Run(ctx context.Context) error {
	if test.GetGlobalTestFlag() {
		return s.startForTest(ctx)
	}

	registerMetrics()

	wg, ctx := errgroup.WithContext(ctx)
	s.workerRtm = worker.NewTaskRunner(defaultRuntimeIncomingQueueLen, defaultRuntimeInitConcurrency)

	wg.Go(func() error {
		return s.workerRtm.Run(ctx)
	})

	err := s.initClients(ctx)
	if err != nil {
		return err
	}
	err = s.selfRegister(ctx)
	if err != nil {
		return err
	}

	// TODO: make the prefix configurable later
	s.resourceBroker = broker.NewBroker(
		&storagecfg.Config{Local: &storagecfg.LocalFileConfig{BaseDir: "./"}},
		s.info.ID,
		s.resourceClient)

	s.p2pMsgRouter = p2p.NewMessageRouter(p2p.NodeID(s.info.ID), s.info.Addr)

	s.grpcSrv = grpc.NewServer()
	err = s.startMsgService(ctx, wg)
	if err != nil {
		return err
	}

	err = s.startTCPService(ctx, wg)
	if err != nil {
		return err
	}

	err = s.fetchMetaStore(ctx)
	if err != nil {
		return err
	}

	s.discoveryKeeper = serverutils.NewDiscoveryKeepaliver(
		s.info, s.etcdCli, s.cfg.SessionTTL, defaultDiscoverTicker,
		s.p2pMsgRouter,
	)
	// connects to metastore and maintains a etcd session
	wg.Go(func() error {
		return s.discoveryKeeper.Keepalive(ctx)
	})

	wg.Go(func() error {
		return s.keepHeartbeat(ctx)
	})

	wg.Go(func() error {
		return s.reportTaskResc(ctx)
	})

	wg.Go(func() error {
		return s.bgUpdateServerMasterClients(ctx)
	})

	wg.Go(func() error {
		return s.collectMetricLoop(ctx, defaultMetricInterval)
	})

	return wg.Wait()
}

// startTCPService starts grpc server and http server
func (s *Server) startTCPService(ctx context.Context, wg *errgroup.Group) error {
	tcpServer, err := tcpserver.NewTCPServer(s.cfg.WorkerAddr, &security.Credential{})
	if err != nil {
		return err
	}
	s.tcpServer = tcpServer
	pb.RegisterExecutorServer(s.grpcSrv, s)
	log.L().Logger.Info("listen address", zap.String("addr", s.cfg.WorkerAddr))

	wg.Go(func() error {
		return s.tcpServer.Run(ctx)
	})

	wg.Go(func() error {
		return s.grpcSrv.Serve(s.tcpServer.GrpcListener())
	})

	wg.Go(func() error {
		return httpHandler(s.tcpServer.HTTP1Listener())
	})
	return nil
}

// current the metastore is an embed etcd underlying
func (s *Server) fetchMetaStore(ctx context.Context) error {
	// query service discovery metastore to fetch metastore connection endpoint
	resp, err := s.masterClient.QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_ServiceDiscovery},
		s.cfg.RPCTimeout,
	)
	if err != nil {
		return err
	}
	log.L().Info("update service discovery metastore", zap.String("addr", resp.Address))

	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:        strings.Split(resp.GetAddress(), ","),
		Context:          ctx,
		LogConfig:        &logConfig,
		DialTimeout:      config.ServerMasterEtcdDialTimeout,
		AutoSyncInterval: config.ServerMasterEtcdSyncInterval,
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		},
	})
	if err != nil {
		return errors.ErrExecutorEtcdConnFail.Wrap(err)
	}
	s.etcdCli = etcdCli

	// fetch framework metastore connection endpoint
	resp, err = s.masterClient.QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_SystemMetaStore},
		s.cfg.RPCTimeout,
	)
	if err != nil {
		return err
	}
	log.L().Info("update framework metastore", zap.String("addr", resp.Address))

	conf := metaclient.StoreConfigParams{
		Endpoints: []string{resp.Address},
	}

	cliEx, err := kvclient.NewKVClient(&conf)
	if err != nil {
		log.L().Error("access framework metastore fail", zap.Any("store-conf", conf), zap.Error(err))
		return err
	}
	// [TODO] use FrameTenantID here if support multi-tenant
	s.metaKVClient = kvclient.NewPrefixKVClient(cliEx, tenant.DefaultUserTenantID)

	// fetch user metastore connection endpoint
	resp, err = s.masterClient.QueryMetaStore(
		ctx,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_AppMetaStore},
		s.cfg.RPCTimeout,
	)
	if err != nil {
		return err
	}
	log.L().Info("update user metastore", zap.String("addr", resp.Address))

	conf = metaclient.StoreConfigParams{
		Endpoints: []string{resp.Address},
	}
	s.userRawKVClient, err = kvclient.NewKVClient(&conf)
	if err != nil {
		log.L().Error("access user metastore fail", zap.Any("store-conf", conf), zap.Error(err))
		return err
	}

	return nil
}

func (s *Server) initClients(ctx context.Context) (err error) {
	s.masterClient, err = client.NewMasterClient(ctx, getJoinURLs(s.cfg.Join))
	if err != nil {
		return err
	}
	log.L().Info("master client init successful")

	resourceCliDialer := func(ctx context.Context, addr string) (pb.ResourceManagerClient, rpcutil.CloseableConnIface, error) {
		ctx, cancel := context.WithTimeout(ctx, client.DialTimeout)
		defer cancel()
		// TODO: reuse connection with masterClient
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return nil, nil, errors.Wrap(errors.ErrGrpcBuildConn, err)
		}
		return pb.NewResourceManagerClient(conn), conn, nil
	}
	s.resourceClient, err = rpcutil.NewFailoverRPCClients[pb.ResourceManagerClient](
		ctx,
		getJoinURLs(s.cfg.Join),
		resourceCliDialer,
	)
	if err != nil {
		if test.GetGlobalTestFlag() {
			log.L().Info("ignore error when in unit tests")
			return nil
		}
		return err
	}
	log.L().Info("resource client init successful")
	return nil
}

func (s *Server) selfRegister(ctx context.Context) (err error) {
	registerReq := &pb.RegisterExecutorRequest{
		Address:    s.cfg.AdvertiseAddr,
		Capability: defaultCapability,
	}
	resp, err := s.masterClient.RegisterExecutor(ctx, registerReq, s.cfg.RPCTimeout)
	if err != nil {
		return err
	}
	s.info = &model.NodeInfo{
		Type:       model.NodeTypeExecutor,
		ID:         model.ExecutorID(resp.ExecutorId),
		Addr:       s.cfg.AdvertiseAddr,
		Capability: int(defaultCapability),
	}
	log.L().Logger.Info("register successful", zap.Any("info", s.info))
	return nil
}

type cliUpdateInfo struct {
	leaderURL string
	urls      []string
}

// TODO: Right now heartbeat maintainable is too simple. We should look into
// what other frameworks do or whether we can use grpc heartbeat.
func (s *Server) keepHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.KeepAliveInterval)
	s.lastHearbeatTime = time.Now()
	defer func() {
		if test.GetGlobalTestFlag() {
			s.testCtx.NotifyExecutorChange(&test.ExecutorChangeEvent{
				Tp:   test.Delete,
				Time: time.Now(),
			})
		}
	}()
	rl := rate.NewLimiter(rate.Every(time.Second*5), 1 /*burst*/)
	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			if s.lastHearbeatTime.Add(s.cfg.KeepAliveTTL).Before(time.Now()) {
				return errors.ErrHeartbeat.GenWithStack("heartbeat timeout")
			}
			req := &pb.HeartbeatRequest{
				ExecutorId: string(s.info.ID),
				Status:     int32(model.Running),
				Timestamp:  uint64(t.Unix()),
				// We set longer ttl for master, which is "ttl + rpc timeout", to avoid that
				// executor actually wait for a timeout when ttl is nearly up.
				Ttl: uint64(s.cfg.KeepAliveTTL.Milliseconds() + s.cfg.RPCTimeout.Milliseconds()),
			}
			resp, err := s.masterClient.Heartbeat(ctx, req, s.cfg.RPCTimeout)
			if err != nil {
				log.L().Error("heartbeat rpc meet error", zap.Error(err))
				if s.lastHearbeatTime.Add(s.cfg.KeepAliveTTL).Before(time.Now()) {
					return errors.Wrap(errors.ErrHeartbeat, err, "rpc")
				}
				continue
			}
			if resp.Err != nil {
				log.L().Warn("heartbeat response meet error", zap.Stringer("code", resp.Err.GetCode()))
				switch resp.Err.Code {
				case pb.ErrorCode_UnknownExecutor, pb.ErrorCode_TombstoneExecutor:
					return errors.ErrHeartbeat.GenWithStack("logic error: %s", resp.Err.GetMessage())
				}
				continue
			}
			// We aim to keep lastHbTime of executor consistent with lastHbTime of Master.
			// If we set the heartbeat time of executor to the start time of rpc, it will
			// be a little bit earlier than the heartbeat time of master, which is safe.
			// In contrast, if we set it to the end time of rpc, it might be a little bit
			// later than master's, which might cause that master wait for less time than executor.
			// This gap is unsafe.
			s.lastHearbeatTime = t
			if rl.Allow() {
				log.L().Info("heartbeat success", zap.String("leader", resp.Leader), zap.Strings("members", resp.Addrs))
			}
			// update master client could cost long time, we make it a background
			// job and if there is running update task, we ignore once since more
			// heartbeats will be called later.

			info := cliUpdateInfo{
				leaderURL: resp.Leader,
				urls:      resp.Addrs,
			}
			select {
			case s.cliUpdateCh <- info:
			default:
			}
		}
	}
}

func getJoinURLs(addrs string) []string {
	return strings.Split(addrs, ",")
}

func (s *Server) reportTaskRescOnce(ctx context.Context) error {
	// TODO: do we need to report allocated resource to master?
	// TODO: Implement task-wise workload reporting in TaskRunner.
	/*
		rescs := s.workerRtm.Workload()
		req := &pb.ExecWorkloadRequest{
			// TODO: use which field as ExecutorId is more accurate
			ExecutorId: s.cfg.WorkerAddr,
			Workloads:  make([]*pb.ExecWorkload, 0, len(rescs)),
		}
		for tp, resc := range rescs {
			req.Workloads = append(req.Workloads, &pb.ExecWorkload{
				Tp:    pb.JobType(tp),
				Usage: int32(resc),
			})
		}
		resp, err := s.masterClient.ReportExecutorWorkload(ctx, req)
		if err != nil {
			return err
		}
		if resp.Err != nil {
			log.L().Warn("report executor workload error", zap.String("err", resp.Err.String()))
		}
	*/
	return nil
}

// reportTaskResc reports tasks resource usage to resource manager periodically
func (s *Server) reportTaskResc(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := s.reportTaskRescOnce(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) bgUpdateServerMasterClients(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case info := <-s.cliUpdateCh:
			s.masterClient.UpdateClients(ctx, info.urls, info.leaderURL)
			s.resourceClient.UpdateClients(ctx, info.urls, info.leaderURL)
		}
	}
}

func (s *Server) collectMetricLoop(ctx context.Context, tickInterval time.Duration) error {
	metricRunningTask := executorTaskNumGauge.WithLabelValues("running")
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			metricRunningTask.Set(float64(s.workerRtm.TaskCount()))
		}
	}
}
