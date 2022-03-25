package statusutil

import (
	"context"

	"github.com/modern-go/reflect2"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

//nolint:structcheck
type Writer[T status[T]] struct {
	metaclient    metaclient.KVClient
	messageSender p2p.MessageSender
	lastStatus    T

	// TODO replace these string types
	workerID   string
	masterInfo MasterInfoProvider
	key        adapter.KeyAdapter
}

func NewWriter[T status[T]](
	metaclient metaclient.KVClient,
	messageSender p2p.MessageSender,
	masterInfo MasterInfoProvider,
	key adapter.KeyAdapter,
	workerID string,
) *Writer[T] {
	return &Writer[T]{
		metaclient:    metaclient,
		messageSender: messageSender,
		masterInfo:    masterInfo,
		key:           key,
		workerID:      workerID,
	}
}

func (w *Writer[T]) UpdateStatus(ctx context.Context, newStatus T) (retErr error) {
	defer func() {
		if retErr == nil {
			return
		}
		log.L().Warn("UpdateStatus failed",
			zap.String("worker-id", w.workerID),
			zap.String("master-id", w.masterInfo.MasterID()),
			zap.String("master-node", w.masterInfo.MasterNode()),
			zap.Int64("master-epoch", w.masterInfo.Epoch()))
	}()

	if reflect2.IsNil(w.lastStatus) || newStatus.HasChanged(w.lastStatus) {
		// Status has changed, so we need to persist the status.
		if err := w.persistStatus(ctx, newStatus); err != nil {
			return err
		}
	}

	topic := WorkerStatusTopic(w.masterInfo.MasterID())
	err := w.messageSender.SendToNodeB(ctx, w.masterInfo.MasterNode(), topic, &WorkerStatusMessage[T]{
		Worker:      w.workerID,
		MasterEpoch: w.masterInfo.Epoch(),
		Status:      newStatus,
	})
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer[T]) persistStatus(ctx context.Context, newStatus T) error {
	raw, err := newStatus.Marshal()
	if err != nil {
		return err
	}

	// TODO handle retry
	if _, err := w.metaclient.Put(ctx, w.key.Encode(w.workerID), string(raw)); err != nil {
		return err
	}
	return nil
}
