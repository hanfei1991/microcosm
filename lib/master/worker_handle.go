package master

import (
	"context"

	libModel "github.com/hanfei1991/microcosm/lib/model"
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

type TombstoneHandle struct {
	entry *workerEntry
}

type RunningWorkerHandle struct {
	messageSender p2p.MessageSender
	entry         *workerEntry
}

func (h *RunningWorkerHandle) SendMessage(
	ctx context.Context,
	topic p2p.Topic,
	message interface{},
	nonblocking bool,
) error {
	executorNodeID := h.entry.ExecutorID

	var err error
	if nonblocking {
		_, err = h.messageSender.SendToNode(ctx, p2p.NodeID(executorNodeID), topic, message)
	} else {
		err = h.messageSender.SendToNodeB(ctx, p2p.NodeID(executorNodeID), topic, message)
	}

	if err != nil {
		return err
	}
	return nil
}
