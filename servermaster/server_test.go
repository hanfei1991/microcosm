package servermaster

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func init() {
	err := log.InitLogger(&log.Config{Level: "warn"})
	if err != nil {
		panic(err)
	}
}

func prepareServerEnv(t *testing.T, name string) (string, *Config, func()) {
	dir, err := ioutil.TempDir("", name)
	require.Nil(t, err)

	ports, err := freeport.GetFreePorts(2)
	require.Nil(t, err)
	cfgTpl := `
master-addr = "127.0.0.1:%d"
advertise-addr = "127.0.0.1:%d"
[etcd]
name = "%s"
data-dir = "%s"
peer-urls = "http://127.0.0.1:%d"
initial-cluster = "%s=http://127.0.0.1:%d"`
	cfgStr := fmt.Sprintf(cfgTpl, ports[0], ports[0], name, dir, ports[1], name, ports[1])
	cfg := NewConfig()
	err = cfg.configFromString(cfgStr)
	require.Nil(t, err)
	err = cfg.adjust()
	require.Nil(t, err)

	cleanupFn := func() {
		os.RemoveAll(dir)
	}
	masterAddr := fmt.Sprintf("127.0.0.1:%d", ports[0])

	return masterAddr, cfg, cleanupFn
}

// Disable parallel run for this case, because prometheus http handler will meet
// data race if parallel run is enabled
func TestStartGrpcSrv(t *testing.T) {
	masterAddr, cfg, cleanup := prepareServerEnv(t, "test-start-grpc-srv")
	defer cleanup()

	s := &Server{cfg: cfg}
	registerMetrics()
	ctx := context.Background()
	err := s.startGrpcSrv(ctx)
	require.Nil(t, err)

	apiURL := fmt.Sprintf("http://%s", masterAddr)
	testPprof(t, apiURL)

	testPrometheusMetrics(t, apiURL)
	s.Stop()
}

func TestStartGrpcSrvCancelable(t *testing.T) {
	t.Parallel()

	dir, err := ioutil.TempDir("", "test-start-grpc-srv-cancelable")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	ports, err := freeport.GetFreePorts(3)
	require.Nil(t, err)
	cfgTpl := `
master-addr = "127.0.0.1:%d"
advertise-addr = "127.0.0.1:%d"
[etcd]
name = "server-master-1"
data-dir = "%s"
peer-urls = "http://127.0.0.1:%d"
initial-cluster = "server-master-1=http://127.0.0.1:%d,server-master-2=http://127.0.0.1:%d"`
	cfgStr := fmt.Sprintf(cfgTpl, ports[0], ports[0], dir, ports[1], ports[1], ports[2])
	cfg := NewConfig()
	err = cfg.configFromString(cfgStr)
	require.Nil(t, err)
	err = cfg.adjust()
	require.Nil(t, err)

	s := &Server{cfg: cfg}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = s.startGrpcSrv(ctx)
	}()
	// sleep a short time to ensure embed etcd is being started
	time.Sleep(time.Millisecond * 100)
	cancel()
	wg.Wait()
	require.EqualError(t, err, context.Canceled.Error())
}

func testPprof(t *testing.T, addr string) {
	urls := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
		// enable these two apis will make ut slow
		//"/debug/pprof/profile", http.MethodGet,
		//"/debug/pprof/trace", http.MethodGet,
		"/debug/pprof/threadcreate",
		"/debug/pprof/allocs",
		"/debug/pprof/block",
		"/debug/pprof/goroutine?debug=1",
		"/debug/pprof/mutex?debug=1",
	}
	for _, uri := range urls {
		resp, err := http.Get(addr + uri)
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_, err = ioutil.ReadAll(resp.Body)
		require.Nil(t, err)
	}
}

func testPrometheusMetrics(t *testing.T, addr string) {
	resp, err := http.Get(addr + "/metrics")
	require.Nil(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = ioutil.ReadAll(resp.Body)
	require.Nil(t, err)
}

func TestCheckLeaderAndNeedForward(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := NewConfig()
	etcdName := "test-check-leader-and-need-forward"
	cfg.Etcd.Name = etcdName
	id := genServerMasterUUID(etcdName)
	s := &Server{id: id, cfg: cfg}
	isLeader, needForward := s.isLeaderAndNeedForward(ctx)
	require.False(t, isLeader)
	require.False(t, needForward)

	var wg sync.WaitGroup
	cctx, cancel := context.WithCancel(ctx)
	startTime := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		isLeader, needForward := s.isLeaderAndNeedForward(cctx)
		require.False(t, isLeader)
		require.False(t, needForward)
	}()
	cancel()
	wg.Wait()
	// should not wait too long time
	require.Less(t, time.Since(startTime), time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()
		isLeader, needForward := s.isLeaderAndNeedForward(ctx)
		require.True(t, isLeader)
		require.True(t, needForward)
	}()
	time.Sleep(time.Second)
	s.leaderClient.Lock()
	s.leaderClient.cli = &client.MasterClientImpl{}
	s.leaderClient.Unlock()
	s.leader.Store(&Member{Name: id})
	wg.Wait()
}

// Server master requires etcd/gRPC service as the minimum running environment,
// this case
// - starts an embed etcd with gRPC service, including message service and
//   server master pb service.
// - campaigns to be leader and then runs leader service.
// Disable parallel run for this case, because prometheus http handler will meet
// data race if parallel run is enabled
func TestRunLeaderService(t *testing.T) {
	_, cfg, cleanup := prepareServerEnv(t, "test-run-leader-service")
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := NewServer(cfg, nil)
	require.Nil(t, err)

	err = s.startGrpcSrv(ctx)
	require.Nil(t, err)

	err = s.reset(ctx)
	require.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.msgService.GetMessageServer().Run(ctx)
	}()

	err = s.campaign(ctx, time.Second)
	require.Nil(t, err)

	ctx1, cancel1 := context.WithTimeout(ctx, time.Second)
	defer cancel1()
	err = s.runLeaderService(ctx1)
	require.EqualError(t, err, context.DeadlineExceeded.Error())

	cancel()
	wg.Wait()
}

type mockJobManager struct {
	lib.BaseMaster
	jobMu sync.RWMutex
	jobs  map[pb.QueryJobResponse_JobStatus]int
}

func (m *mockJobManager) JobCount(status pb.QueryJobResponse_JobStatus) int {
	m.jobMu.RLock()
	defer m.jobMu.RUnlock()
	return m.jobs[status]
}

func (m *mockJobManager) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	panic("not implemented")
}

func (m *mockJobManager) PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse {
	panic("not implemented")
}

type mockExecutorManager struct {
	executorMu sync.RWMutex
	count      map[model.ExecutorStatus]int
}

func (m *mockExecutorManager) IsExecutorAlive(id model.ExecutorID) bool {
	panic("implement me")
}

func (m *mockExecutorManager) SetExecutorChangedHandler(handler ExecutorChangeHandler) {
	panic("implement me")
}

func (m *mockExecutorManager) HandleHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	panic("not implemented")
}

func (m *mockExecutorManager) Allocate(ctx context.Context, task *pb.ScheduleTask) (bool, *pb.ScheduleResult) {
	panic("not implemented")
}

func (m *mockExecutorManager) AllocateNewExec(req *pb.RegisterExecutorRequest) (*model.NodeInfo, error) {
	panic("not implemented")
}

func (m *mockExecutorManager) RegisterExec(info *model.NodeInfo) {
	panic("not implemented")
}

func (m *mockExecutorManager) Start(ctx context.Context) {
	panic("not implemented")
}

func (m *mockExecutorManager) ExecutorCount(status model.ExecutorStatus) int {
	m.executorMu.RLock()
	defer m.executorMu.RUnlock()
	return m.count[status]
}

func TestCollectMetric(t *testing.T) {
	masterAddr, cfg, cleanup := prepareServerEnv(t, "test-collect-metric")
	defer cleanup()

	registerMetrics()
	s := &Server{
		cfg:     cfg,
		metrics: newServerMasterMetric(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := s.startGrpcSrv(ctx)
	require.Nil(t, err)

	jobManager := &mockJobManager{
		jobs: map[pb.QueryJobResponse_JobStatus]int{
			pb.QueryJobResponse_online: 3,
		},
	}
	executorManager := &mockExecutorManager{
		count: map[model.ExecutorStatus]int{
			model.Initing: 1,
			model.Running: 2,
		},
	}
	s.jobManager = jobManager
	s.executorManager = executorManager

	s.collectLeaderMetric()
	apiURL := fmt.Sprintf("http://%s", masterAddr)
	testCustomedPrometheusMetrics(t, apiURL)
	s.Stop()
	cancel()
}

func testCustomedPrometheusMetrics(t *testing.T, addr string) {
	require.Eventually(t, func() bool {
		resp, err := http.Get(addr + "/metrics")
		require.Nil(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		body, err := ioutil.ReadAll(resp.Body)
		require.Nil(t, err)
		metric := string(body)
		return strings.Contains(metric, "dataflow_server_master_job_num") &&
			strings.Contains(metric, "dataflow_server_master_executor_num")
	}, time.Second, time.Millisecond*20)
}
