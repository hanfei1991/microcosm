package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib/fake"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

type utCli struct {
	// used to operate with server master, such as submit job
	masterCli client.MasterClient
	// used to query metadata which is stored from business logic
	metaCli metaclient.KVClient
	// used to write to etcd to simulate the business of fake job, this etcd client
	// reuses the endpoints of user meta KV.
	fakeJobCli *clientv3.Client
	fakeJobCfg *fakeJobConfig
}

type fakeJobConfig struct {
	etcdEndpoints []string
	workerCount   int
	keyPrefix     string
}

func NewUTCli(
	ctx context.Context, masterAddrs, userMetaAddrs []string, cfg *fakeJobConfig,
) (*utCli, error) {
	masterCli, err := client.NewMasterClient(ctx, masterAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	conf := metaclient.StoreConfigParams{Endpoints: userMetaAddrs}
	userRawKVClient, err := kvclient.NewKVClient(&conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	metaCli := kvclient.NewPrefixKVClient(userRawKVClient, tenant.DefaultUserTenantID)

	fakeJobCli, err := clientv3.New(clientv3.Config{
		Endpoints:   userMetaAddrs,
		Context:     ctx,
		DialTimeout: 3 * time.Second,
		DialOptions: []grpc.DialOption{},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &utCli{
		masterCli:  masterCli,
		metaCli:    metaCli,
		fakeJobCli: fakeJobCli,
		fakeJobCfg: cfg,
	}, nil
}

func (cli *utCli) createJob(ctx context.Context, tp pb.JobType, config []byte) (string, error) {
	req := &pb.SubmitJobRequest{Tp: tp, Config: config}
	resp, err := cli.masterCli.SubmitJob(ctx, req)
	if err != nil {
		return "", err
	}
	if resp.Err != nil {
		return "", errors.New(resp.Err.String())
	}
	return resp.JobIdStr, nil
}

func (cli *utCli) pauseJob(ctx context.Context, jobID string) error {
	req := &pb.PauseJobRequest{JobIdStr: jobID}
	resp, err := cli.masterCli.PauseJob(ctx, req)
	if err != nil {
		return err
	}
	if resp.Err != nil {
		return errors.New(resp.Err.String())
	}
	return nil
}

func (cli *utCli) checkJobStatus(
	ctx context.Context, jobID string, expectedStatus pb.QueryJobResponse_JobStatus,
) (bool, error) {
	req := &pb.QueryJobRequest{JobId: jobID}
	resp, err := cli.masterCli.QueryJob(ctx, req)
	if err != nil {
		return false, err
	}
	if resp.Err != nil {
		return false, errors.New(resp.Err.String())
	}
	return resp.Status == expectedStatus, nil
}

func (cli *utCli) updateFakeJobKey(ctx context.Context, id int, value string) error {
	key := fmt.Sprintf("%s%d", cli.fakeJobCfg.keyPrefix, id)
	_, err := cli.fakeJobCli.Put(ctx, key, value)
	return errors.Trace(err)
}

func (cli *utCli) getFakeJobCheckpoint(
	ctx context.Context, masterID string, jobIndex int,
) (*fake.Checkpoint, error) {
	ckptKey := fake.CheckpointKey(masterID)
	resp, metaErr := cli.metaCli.Get(ctx, ckptKey)
	if metaErr != nil {
		return nil, errors.New(metaErr.Error())
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("no checkpoint found")
	}
	checkpoint := &fake.Checkpoint{}
	err := json.Unmarshal(resp.Kvs[0].Value, checkpoint)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return checkpoint, nil
}

func (cli *utCli) checkFakeJobTick(
	ctx context.Context, masterID string, jobIndex int, target int64,
) error {
	ckpt, err := cli.getFakeJobCheckpoint(ctx, masterID, jobIndex)
	if err != nil {
		return err
	}
	if tick, ok := ckpt.Ticks[jobIndex]; !ok {
		return errors.Errorf("job %d not found in checkpoint %v", jobIndex, ckpt)
	} else {
		if tick < target {
			return errors.Errorf("tick %d not reaches target %d", tick, target)
		}
	}
	return nil
}

func (cli *utCli) checkFakeJobKey(
	ctx context.Context, masterID string, jobIndex int, expectedMvcc int, expectedValue string,
) error {
	checkpoint, err := cli.getFakeJobCheckpoint(ctx, masterID, jobIndex)
	if err != nil {
		return err
	}
	if ckpt, ok := checkpoint.EtcdCheckpoints[jobIndex]; !ok {
		return errors.Errorf("job %d not found in checkpoint %v", jobIndex, checkpoint)
	} else {
		if ckpt.Value != expectedValue {
			return errors.Errorf("value not equals, expected: %s, actual: %s", expectedValue, ckpt.Value)
		}
		if ckpt.Mvcc != expectedMvcc {
			return errors.Errorf("mvcc not equals, expected: %d, actual: %d", expectedMvcc, ckpt.Mvcc)
		}
	}

	return nil
}

func TestNodeFailure(t *testing.T) {
	// TODO: make the following variables configurable
	var (
		masterAddrs              = []string{"127.0.0.1:10245", "127.0.0.1:10246", "127.0.0.1:10247"}
		userMetaAddrs            = []string{"127.0.0.1:12479"}
		userMetaAddrsInContainer = []string{"user-etcd-standalone:2379"}
	)

	ctx := context.Background()
	cfg := &fake.Config{
		JobName:     "test-node-failure",
		WorkerCount: 5,
		// use a large enough target tick to ensure the fake job long running
		TargetTick:      10000000,
		EtcdWatchEnable: true,
		EtcdEndpoints:   userMetaAddrsInContainer,
		EtcdWatchPrefix: "/fake-job/test/",
	}
	cfgBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	fakeJobCfg := &fakeJobConfig{
		etcdEndpoints: userMetaAddrs, // reuse user meta KV endpoints
		workerCount:   cfg.WorkerCount,
		keyPrefix:     cfg.EtcdWatchPrefix,
	}
	cli, err := NewUTCli(ctx, masterAddrs, userMetaAddrs, fakeJobCfg)
	require.NoError(t, err)

	jobID, err := cli.createJob(ctx, pb.JobType_FakeJob, cfgBytes)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		// check tick increases to ensure all workers are online
		targetTick := int64(20)
		for jobIdx := 0; jobIdx < cfg.WorkerCount; jobIdx++ {
			err := cli.checkFakeJobTick(ctx, jobID, jobIdx, targetTick)
			if err != nil {
				log.L().Warn("check fake job tick failed", zap.Error(err))
				return false
			}
		}
		return true
	}, time.Second*30, time.Second)

	sourceUpdateCount := 10
	sourceValue := "value"
	for i := 0; i < sourceUpdateCount; i++ {
		for j := 0; j < cfg.WorkerCount; j++ {
			err := cli.updateFakeJobKey(ctx, j, sourceValue)
			require.NoError(t, err)
		}
	}

	require.Eventually(t, func() bool {
		for jobIdx := 0; jobIdx < cfg.WorkerCount; jobIdx++ {
			err := cli.checkFakeJobKey(ctx, jobID, jobIdx, sourceUpdateCount, sourceValue)
			if err != nil {
				log.L().Warn("check fake job failed", zap.Error(err))
				return false
			}
		}
		return true
	}, time.Second*30, time.Second)

	err = cli.pauseJob(ctx, jobID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		stopped, err := cli.checkJobStatus(ctx, jobID, pb.QueryJobResponse_stopped)
		return err == nil && stopped
	}, time.Second*30, time.Second)
}
