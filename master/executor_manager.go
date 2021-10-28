package master

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


type Executor struct {
	model.ExecutorInfo

	lastUpdateTime time.Time
}
