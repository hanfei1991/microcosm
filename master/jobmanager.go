package master

import (
	"errors"

	"github.com/hanfei1991/microcosom/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosom/master/scheduler"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/etcdutil"
	"go.etcd.io/etcd/clientv3"
)


type JobManager struct {
	cli *clientv3.Client

	jobMasters []JobMaster
}

func (j *JobManager) SubmitJob(req *pb.SubmitJobRequest, s *scheduler.Scheduler) (error) {
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
	j.jobMasters = append(j.jobMasters, jobMaster)
	return jobMaster.Dispatch(s)
}
