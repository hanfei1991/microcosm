package statusutil

import (
	"context"
	"time"

	"github.com/modern-go/reflect2"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// Writer is used to persist WorkerStatus changes and send notifications
// to the Master.
type Writer struct {
	metaclient    pkgOrm.Client
	messageSender p2p.MessageSender
	lastStatus    *libModel.WorkerStatus

	// TODO replace the string type
	workerID   string
	masterInfo MasterInfoProvider
}

// NewWriter creates a new Writer.
func NewWriter(
	metaclient pkgOrm.Client,
	messageSender p2p.MessageSender,
	masterInfo MasterInfoProvider,
	workerID string,
) *Writer {
	return &Writer{
		metaclient:    metaclient,
		messageSender: messageSender,
		masterInfo:    masterInfo,
		workerID:      workerID,
	}
}

// UpdateStatus checks if newStatus.HasSignificantChange() is true, if so, it persists the change and
// tries to send a notification. Note that sending the notification is asynchronous.
func (w *Writer) UpdateStatus(ctx context.Context, newStatus *libModel.WorkerStatus) (retErr error) {
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

	if reflect2.IsNil(w.lastStatus) || newStatus.HasSignificantChange(w.lastStatus) {
		// Status has changed, so we need to persist the status.
		if err := w.persistStatus(ctx, newStatus); err != nil {
			return err
		}
	}

	w.lastStatus = newStatus

	// TODO replace the timeout with a variable.
	return w.sendStatusMessageWithRetry(ctx, 15*time.Second, newStatus)
}

func (w *Writer) sendStatusMessageWithRetry(
	ctx context.Context, timeout time.Duration, newStatus *libModel.WorkerStatus,
) error {
	// NOTE we need this function especially to handle the situation where
	// the p2p connection to the target executor is not established yet.
	// We might need one or two retries when our executor has just started up.

	retryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rl := rate.NewLimiter(rate.Limit(100*time.Millisecond), 1)
	for {
		select {
		case <-retryCtx.Done():
			return errors.Trace(retryCtx.Err())
		default:
		}

		if err := rl.Wait(retryCtx); err != nil {
			return errors.Trace(err)
		}

		topic := WorkerStatusTopic(w.masterInfo.MasterID())
		// NOTE: We must ready the MasterNode() in each retry in case the master is failed over.
		err := w.messageSender.SendToNodeB(ctx, w.masterInfo.MasterNode(), topic, &WorkerStatusMessage{
			Worker:      w.workerID,
			MasterEpoch: w.masterInfo.Epoch(),
			Status:      newStatus,
		})
		if err != nil {
			if derrors.ErrExecutorNotFoundForMessage.Equal(err) {
				if err := w.masterInfo.RefreshMasterInfo(ctx); err != nil {
					log.L().Warn("failed to refresh master info",
						zap.String("worker-id", w.workerID),
						zap.String("master-id", w.masterInfo.MasterID()),
						zap.Error(err))
				}
			}
			log.L().Warn("failed to send status to master. Retrying...",
				zap.String("worker-id", w.workerID),
				zap.String("master-id", w.masterInfo.MasterID()),
				zap.Any("status", newStatus),
				zap.Error(err))
			continue
		}
		return nil
	}
}

func (w *Writer) persistStatus(ctx context.Context, newStatus *libModel.WorkerStatus) error {
	return retry.Do(ctx, func() error {
		return w.metaclient.UpdateWorker(ctx, newStatus)
	}, retry.WithBackoffMaxDelay(1000 /* 1 second */), retry.WithIsRetryableErr(func(err error) bool {
		// TODO: refine the IsRetryable method
		//if err, ok := err.(metaclient.Error); ok {
		//return err.IsRetryable()
		//}
		return true
	}))
}
