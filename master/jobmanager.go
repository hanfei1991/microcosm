package master

import (
	"errors"
	"time"

	"github.com/hanfei1991/microcosom/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/etcdutil"
	"go.etcd.io/etcd/clientv3"
)


type JobManager struct {
	cli *clientv3.Client

	jobMasters []JobMaster
	dispatchJobQueue chan JobMaster
	resourceMgr cluster.ResourceMgr
	executorClient cluster.ExecutorClient
}

func (j *JobManager) SubmitJob(req *pb.SubmitJobRequest) (error) {
	info := model.JobInfo{
		Config: req.Config,
		UserName: req.User,
	}
	var jobMaster JobMaster	
	var err error
	switch req.Tp {
	case pb.SubmitJobRequest_Benchmark:
		info.Type = model.JobBenchmark
		jobMaster, err = benchmark.BuildBenchmarkJobMaster(info.Config)
		if err != nil {
			return err
		}
	default:
		return errors.New("not yet implemented")
	}
	j.jobMasters[jobMaster.ID()] = jobMaster
	j.dispatchJobQueue <- jobMaster
	return nil
}

func (j *JobManager) Run() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <- ticker.C:
			// 
			task := j.resourceMgr.GetRescheduleTask()
			if task != nil {
				j.jobMasters[task.JobID].RescheduleTask(task)
			}
		case jobMaster := <- j.dispatchJobQueue:
			jobMaster.DispatchJob()
		}
	}
}
