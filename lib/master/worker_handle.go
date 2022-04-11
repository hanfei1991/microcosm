package master

import (
	"context"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type WorkerHandle interface {
	Status() *libModel.WorkerStatus
	ID() libModel.WorkerID
	GetTombstone() TombstoneHandle
	Unwrap() RunningHandle
	ToPB() (*pb.WorkerInfo, error)
}

type RunningHandle interface {
	Status() *libModel.WorkerStatus
	ID() libModel.WorkerID
	ToPB() (*pb.WorkerInfo, error)
	SendMessage(
		ctx context.Context,
		topic p2p.Topic,
		message interface{},
		nonblocking bool,
	) error
}

type TombstoneHandle interface {
	Status() *libModel.WorkerStatus
	ID() libModel.WorkerID
	ToPB() (*pb.WorkerInfo, error)
	CleanTombstone(ctx context.Context) error
}

type runningHandleImpl struct {
	workerID   libModel.WorkerID
	executorID model.ExecutorID
	manager    *WorkerManager
}

func (h *runningHandleImpl) Status() *libModel.WorkerStatus {
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()

	entry, exists := h.manager.workerEntries[h.workerID]
	if !exists {
		log.L().Panic("Using a stale handle", zap.String("worker-id", h.workerID))
	}

	return entry.StatusReader().Status()
}

func (h *runningHandleImpl) ID() libModel.WorkerID {
	return h.workerID
}

func (h *runningHandleImpl) GetTombstone() TombstoneHandle {
	return nil
}

func (h *runningHandleImpl) Unwrap() RunningHandle {
	return h
}

func (h *runningHandleImpl) ToPB() (*pb.WorkerInfo, error) {
	statusBytes, err := h.Status().Marshal()
	if err != nil {
		return nil, err
	}

	ret := &pb.WorkerInfo{
		Id:         h.workerID,
		ExecutorId: string(h.executorID),
		Status:     statusBytes,
	}
	return ret, nil
}

func (h *runningHandleImpl) SendMessage(
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

type tombstoneHandleImpl struct {
	workerID libModel.WorkerID
	manager  *WorkerManager
}

func (h *tombstoneHandleImpl) Status() *libModel.WorkerStatus {
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()

	entry, exists := h.manager.workerEntries[h.workerID]
	if !exists {
		log.L().Panic("Using a stale handle", zap.String("worker-id", h.workerID))
	}

	return entry.StatusReader().Status()
}

func (h *tombstoneHandleImpl) ID() libModel.WorkerID {
	return h.workerID
}

func (h *tombstoneHandleImpl) GetTombstone() TombstoneHandle {
	return h
}

func (h *tombstoneHandleImpl) Unwrap() RunningHandle {
	return nil
}

func (h *tombstoneHandleImpl) ToPB() (*pb.WorkerInfo, error) {
	return nil, nil
}

func (h *tombstoneHandleImpl) CleanTombstone(ctx context.Context) error {
	panic("implement me")
}
