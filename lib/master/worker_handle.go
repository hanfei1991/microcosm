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
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()

	entry, exists := h.manager.workerEntries[h.workerID]
	if !exists {
		log.L().Panic("Using a stale handle", zap.String("worker-id", h.workerID))
	}

	return entry.StatusReader().Status()
}

func (h *RunningWorkerHandle) ID() libModel.WorkerID {
	return h.workerID
}

func (h *RunningWorkerHandle) GetTombstone() *TombstoneHandle {
	return nil
}

func (h *RunningWorkerHandle) Unwrap() *RunningWorkerHandle {
	return h
}

func (h *RunningWorkerHandle) ToPB() (*pb.WorkerInfo, error) {
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

func (h *TombstoneHandle) Status() *libModel.WorkerStatus {
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()

	entry, exists := h.manager.workerEntries[h.workerID]
	if !exists {
		log.L().Panic("Using a stale handle", zap.String("worker-id", h.workerID))
	}

	return entry.StatusReader().Status()
}

func (h *TombstoneHandle) ID() libModel.WorkerID {
	return h.workerID
}

func (h *TombstoneHandle) GetTombstone() *TombstoneHandle {
	return h
}

func (h *TombstoneHandle) Unwrap() *RunningWorkerHandle {
	return nil
}

func (h *TombstoneHandle) ToPB() (*pb.WorkerInfo, error) {
	return nil, nil
}
