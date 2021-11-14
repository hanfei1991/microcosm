package master

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/autoid"
	"github.com/hanfei1991/microcosom/pkg/log"
	"go.uber.org/zap"
)

// JobManager manages all the job masters, and notifys the offline executor to them.
type JobManager struct {
	mu                sync.Mutex
	jobMasters        map[model.JobID]JobMaster

	idAllocater    *autoid.Allocator
	resourceMgr    cluster.ResourceMgr
	executorClient cluster.ExecutorClient

	offExecutors chan model.ExecutorID
}

// Start the deamon gouroutine.
func (j *JobManager) Start(ctx context.Context) {
	go j.startImpl(ctx)
}

func (j *JobManager) startImpl(ctx context.Context) {
	for {
		select {
		case execID := <-j.offExecutors:
			log.L().Logger.Info("notify to offline exec")
			j.mu.Lock()
			for _, jobMaster := range j.jobMasters {
				jobMaster.OfflineExecutor(execID)
			}
			j.mu.Unlock()
		case <-ctx.Done():
		}
	}
}

// SubmitJob processes "SubmitJobRequest".
func (j *JobManager) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	info := model.JobInfo{
		Config:   string(req.Config),
		UserName: req.User,
	}
	var jobMaster JobMaster
	var err error
	log.L().Logger.Info("submit job", zap.String("config", info.Config))
	resp := &pb.SubmitJobResponse{}
	switch req.Tp {
	case pb.SubmitJobRequest_Benchmark:
		info.Type = model.JobBenchmark
		jobMaster, err = benchmark.BuildBenchmarkJobMaster(ctx, info.Config, j.idAllocater, j.resourceMgr, j.executorClient)
		if err != nil {
			resp.ErrMessage = err.Error()
			return resp
		}
	default:
		resp.ErrMessage = "unknown job type"
		return resp
	}
	log.L().Logger.Info("finished build job")
	j.mu.Lock()
	defer j.mu.Unlock()
	err = jobMaster.DispatchJob()
	if err != nil {
		resp.ErrMessage = err.Error()
		return resp
	}
	j.jobMasters[jobMaster.ID()] = jobMaster

	resp.JobId = int32(jobMaster.ID())
	return resp
}
