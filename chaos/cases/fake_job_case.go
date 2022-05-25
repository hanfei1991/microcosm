package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hanfei1991/microcosm/lib/fake"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test/e2e"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

func runFakeJobCase(ctx context.Context, cfg *config) error {
	serverMasterEndpoints := []string{cfg.MasterAddr}
	etcdEndpoints := []string{cfg.EtcdAddr}

	jobCfg := &fake.Config{
		JobName:     "fake-job-case",
		WorkerCount: 8,
		// use a large enough target tick to ensure the fake job long running
		TargetTick:      10000000,
		EtcdWatchEnable: true,
		EtcdEndpoints:   etcdEndpoints,
		EtcdWatchPrefix: "/fake-job/test/",
	}
	e2eCfg := &e2e.FakeJobConfig{
		EtcdEndpoints: etcdEndpoints, // reuse user meta KV endpoints
		WorkerCount:   jobCfg.WorkerCount,
		KeyPrefix:     jobCfg.EtcdWatchPrefix,
	}
	cli, err := e2e.NewUTCli(ctx, serverMasterEndpoints, etcdEndpoints, e2eCfg)
	if err != nil {
		return err
	}
	revision, err := cli.GetRevision(ctx)
	if err != nil {
		return err
	}
	jobCfg.EtcdStartRevision = revision
	cfgBytes, err := json.Marshal(jobCfg)
	if err != nil {
		return err
	}

	// create a fake job
	jobID, err := cli.CreateJob(ctx, pb.JobType_FakeJob, cfgBytes)
	if err != nil {
		return err
	}

	// update upstream etcd, and check fake job works normally every 30 seconds
	// run 20 times, about 10 minutes totally.
	mvcc := 0
	interval := 30 * time.Second
	runTime := 20
	for i := 0; i < runTime; i++ {
		value := fmt.Sprintf("update-value-index-%d", i)
		mvcc++
		start := time.Now()
		err := updateKeyAndCheck(ctx, cli, jobID, jobCfg.WorkerCount, value, mvcc)
		if err != nil {
			return err
		}
		duration := time.Since(start)
		log.L().Info("update key and check test", zap.Int("round", i), zap.Duration("duration", duration))
		if duration < interval {
			time.Sleep(start.Add(interval).Sub(time.Now()))
		}
	}

	log.L().Info("run fake job case successfully")

	return nil
}

func updateKeyAndCheck(
	ctx context.Context, cli *e2e.ChaosCli, jobID string, workerCount int,
	updateValue string, expectedMvcc int,
) error {
	for i := 0; i < workerCount; i++ {
		err := cli.UpdateFakeJobKey(ctx, i, updateValue)
		if err != nil {
			return err
		}
	}
	finished := util.WaitSomething(60, time.Second*5, func() bool {
		for jobIdx := 0; jobIdx < workerCount; jobIdx++ {
			err := cli.CheckFakeJobKey(ctx, jobID, jobIdx, expectedMvcc, updateValue)
			if err != nil {
				log.L().Warn("check fail job failed", zap.Error(err))
				return false
			}
		}
		return true
	})
	if !finished {
		return errors.New("wait fake job normally timeout")
	}
	return nil
}
