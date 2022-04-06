package master

import (
	"context"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type WorkerHandle interface {
	Status() *libModel.WorkerStatus
	ID() libModel.WorkerID
	GetTombstone() *TombstoneHandle
	Unwrap() *RunningWorkerHandle
	ToPB() (*pb.WorkerInfo, error)
}

type RunningWorkerHandle struct {
	workerID   libModel.WorkerID
	executorID model.ExecutorID
	manager    *WorkerManager
}

func (h *RunningWorkerHandle) Status() *libModel.WorkerStatus {
	// TODO implement me
	panic("implement me")
}

func (h *RunningWorkerHandle) ID() libModel.WorkerID {
	// TODO implement me
	panic("implement me")
}

func (h *RunningWorkerHandle) GetTombstone() *TombstoneHandle {
	// TODO implement me
	panic("implement me")
}

func (h *RunningWorkerHandle) Unwrap() *RunningWorkerHandle {
	// TODO implement me
	panic("implement me")
}

func (h *RunningWorkerHandle) ToPB() (*pb.WorkerInfo, error) {
	// TODO implement me
	panic("implement me")
}

func (h *RunningWorkerHandle) SendMessage(
	ctx context.Context,
	topic p2p.Topic,
	message interface{},
	nonblocking bool,
) error {
	var err error
	if nonblocking {
		_, err = h.manager.messageSender.SendToNode(ctx, p2p.NodeID(h.executorID), topic, message)
	} else {
		err = h.manager.messageSender.SendToNodeB(ctx, p2p.NodeID(h.executorID), topic, message)
	}

	if err != nil {
		return err
	}
	return nil
}

type TombstoneHandle struct {
	workerID libModel.WorkerID
	manager  *WorkerManager
}

func (t TombstoneHandle) Status() *libModel.WorkerStatus {
	// TODO implement me
	panic("implement me")
}

func (t TombstoneHandle) ID() libModel.WorkerID {
	// TODO implement me
	panic("implement me")
}

func (t TombstoneHandle) GetTombstone() *TombstoneHandle {
	// TODO implement me
	panic("implement me")
}

func (t TombstoneHandle) Unwrap() *RunningWorkerHandle {
	// TODO implement me
	panic("implement me")
}

func (t TombstoneHandle) ToPB() (*pb.WorkerInfo, error) {
	// TODO implement me
	panic("implement me")
}
