package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/etcdutil"
	"github.com/hanfei1991/microcosom/pkg/log"
	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

type Scheduler struct {
	// TODO: We should abstract a ha store to handle.
	etcdCli *clientv3.Client

	mu sync.Mutex
	executors map[string]*Executor
}

func (s *Scheduler) UpdateExecutor() {

}

func (s *Scheduler) AddExecutor(req *pb.RegisterExecutorRequest) error {
	info := model.ExecutorInfo{
		Name: req.Name,
		Addr: req.Address,
		Capability: int(req.Capability),
	}

	s.mu.Lock()
	if e, ok := s.executors[info.Name]; ok {
		s.mu.Unlock()
		if e.Addr != req.Address {
			return errors.Errorf("Executor %s has been registered", info.Name)
		}
		// TODO: If executor is pulled up again after crashed, we should recover it 
		// by dispatching old tasks again.
		// info.Status = model.ExecutorRecovering
		// err = recover(info)
		// if err != nil {return err}
	}
	s.mu.Unlock()	

	// Following part is to bootstrap the executor.
	info.Status = int(model.ExecutorBootstrap)

	value, err := info.ToJSON()
	if err != nil {
		return err
	}

	e := &Executor{ExecutorInfo: info}
	err = e.Initialize()
	if err != nil {
		return err
	}
	// Persistant
	_,_, err = etcdutil.DoOpsInOneTxnWithRetry(s.etcdCli, clientv3.OpPut(ExecutorKeyAdapter.Encode(info.Name), value))
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.executors[info.Name] = e
	s.mu.Unlock()
	return nil
}

func (s *Scheduler) naiveSchedule(job *model.Job) error {
	for _, task := range job.Tasks {
		hasAlloced := false
		for _, executor := range s.executors {
			if executor.Status != int(model.ExecutorRunning) {
				continue
			}
			if task.Cost + executor.workload.Usage < executor.Capability {
				err := s.AddTask(executor, job.ID, task)
				if err != nil {
					// log error
					continue
				} else {
					hasAlloced = true
					break
				}
			}
		}
		if !hasAlloced {
			return errors.New("no free executor for job")
		}
	}
	return nil
}

func (s *Scheduler) dispatchJob(jobID model.JobID) (error) {
	// We dispatch tasks by random order.
	// TODO: for connected task, we should dispatch task by topological order.
	for _, e := range s.executors {
		if job, ok := e.jobs[jobID]; ok {
			reSchedule, err := s.submitSubJob(e, job)	
			if err != nil {
				// TODO: retry or not
				// TODO: If there is a communication error, set the executor to "fault" status.
				if !reSchedule {
					return err
				} else {
					delete(e.jobs, jobID)
					err = s.Schedule(job)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (s *Scheduler) submitSubJob(e *Executor, job *model.Job) (bool, error) {
	req := job.ToPB()
	_, err := e.client.SubmitSubJob(context.Background(), req)
	// TODO: check the err msg
	if err != nil {
		return false, err
	}

	return false, nil
}



func (s *Scheduler) Schedule(job *model.Job) (error) {
	// add schedule lock and schedule the job
	s.naiveSchedule(job)
	//  dispatch the job
	return s.dispatchJob(job.ID)
}

func (s *Scheduler) Launch(job *model.Job) error {
	for _, e := range s.executors {
		if job, ok := e.jobs[job.ID]; ok {
			req := &pb.LaunchSubJobRequest{
				JobId: int32(job.ID),
			}
			// grpc request
			resp, err := e.client.LaunchSubJob(context.Background(), req)
			if err != nil {
				return err
			}
		}
	}
	return nil
}


type Executor struct {
	model.ExecutorInfo

	workload 	   ExecutorWorkload
	lastUpdateTime time.Time

	jobs map[model.JobID]*model.Job

	client pb.ExecutorClient
}

func (s *Scheduler) AddTask(e *Executor, jobID model.JobID, task *model.Task) error {
	e.workload.Usage += task.Cost
	task.Executor = e.Name
	job, ok := e.jobs[jobID]
	if !ok {
		job = &model.Job{
			ID: jobID,
			Tasks: make(map[int]*model.Task),
		}
		e.jobs[jobID] = job
	}
	job.Tasks[task.ID] = task
	// ha put task
	return nil
}

func (e *Executor) Initialize() error {
	conn, err := grpc.Dial(e.Addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return err
	}
	e.client = pb.NewExecutorClient(conn)
}

type ExecutorWorkload struct {
	Usage int
}