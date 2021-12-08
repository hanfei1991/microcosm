package benchmark

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/master/cluster"
	"github.com/hanfei1991/microcosm/master/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/errors"
)

func syncerCallback(m *jobMaster, senderId string, req interface{}) error {
	return m.ddl.resolveDDL(context.Background(), req.(*SyncDDLRequest).Id, senderId, m.Client)
}

type ddlResolver struct {
	sync.Mutex
	ongoingTasks map[model.TaskID]struct{}
	ddlSize int

	finishCh chan struct{}
	errCh chan error
	err error
}

func (r *ddlResolver) finishAndReset() {
	close(r.finishCh)
	r.ongoingTasks = make(map[model.TaskID]struct{})
	r.finishCh = make(chan struct{})
}

func (r *ddlResolver) resolveDDL(ctx context.Context, id model.TaskID, senderId string, client cluster.ExecutorClient) error {
	r.Lock()
	_, ok := r.ongoingTasks[id]
	if ok {
		r.Unlock()
		r.err = errors.ErrBenchmarkDuplicateSyncDDLReq.FastGenByArgs(id)
		close(r.finishCh)
		return r.err
	}
	r.ongoingTasks[id] = struct{}{}
	if len(r.ongoingTasks) == r.ddlSize {
		r.finishAndReset()	
		r.Unlock()
		return nil
	}
	r.Unlock()
	go func() {
		select {
		case <-r.finishCh:
			resp := &SyncDDLResponse {
				TaskID: id,
				Err: r.err.Error(),
			}
			client.SendMessage(ctx, senderId, "SyncDDL", resp)
		}
	}()
	return nil
}

type jobMaster struct {
	*system.Master
	config *Config

	stage1 []*model.Task
	stage2 []*model.Task

	ddl    *ddlResolver
	p2pSender system.MessageServer
}

var callBackMap map[model.OperatorType] func(*jobMaster, )

func (m *jobMaster) Start(ctx context.Context) error {
	m.StartInternal()
	// start stage1
	err := m.DispatchTasks(ctx, m.stage1)
	// start stage2
	if err != nil {
		return err
	}

	err = m.DispatchTasks(ctx, m.stage2)
	if err != nil {
		return err
	}
	// TODO: Start the tasks manager to communicate.
	return nil
}
