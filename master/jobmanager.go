package master

import (
	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/autoid"
	"github.com/hanfei1991/microcosom/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)


type JobManager struct {
	cli *clientv3.Client

	jobMasters map[model.JobID]JobMaster
	dispatchJobQueue chan JobMaster
	scheduleTaskQueue chan *model.Task

	idAllocater *autoid.Allocator
	resourceMgr cluster.ResourceMgr
	executorClient cluster.ExecutorClient
}

// SubmitJob processes "SubmitJobRequest".
func (j *JobManager) SubmitJob(req *pb.SubmitJobRequest) (*pb.SubmitJobResponse) {
	info := model.JobInfo{
		Config: string(req.Config),
		UserName: req.User,
	}
	var jobMaster JobMaster	
	var err error
	log.L().Logger.Info("submit job", zap.String("config", info.Config))
	resp := &pb.SubmitJobResponse{}
	switch req.Tp {
	case pb.SubmitJobRequest_Benchmark:
		info.Type = model.JobBenchmark
		jobMaster, err = benchmark.BuildBenchmarkJobMaster(info.Config, j.idAllocater, j.resourceMgr, j.executorClient)
		if err != nil {
			resp.ErrMessage = err.Error()
			return resp
		}
	default:
		resp.ErrMessage = "unknown job type"
		return resp
	}
	err = jobMaster.DispatchJob()
	if err != nil {
		resp.ErrMessage = err.Error()
		return resp
	}
	j.jobMasters[jobMaster.ID()] = jobMaster
	resp.JobId = int32(jobMaster.ID())
	return resp
}
