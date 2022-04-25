package client

import (
	"context"
	"time"

	"github.com/gogo/status"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"

	"github.com/hanfei1991/microcosm/pb"
)

const preDispatchTaskRetryInterval = 1 * time.Second

// TaskDispatcher implements the logic to invoke two-phase task-dispatching.
// A separate struct is used to decouple the complexity of the two-phase
// protocol from the implementation of ExecutorClient.
// TODO think about whether we should refactor the ExecutorClient's interface.
type TaskDispatcher struct {
	client ExecutorClient

	timeout time.Duration
}

// DispatchTaskArgs contains the required parameters for creating a worker.
type DispatchTaskArgs struct {
	WorkerID     string
	MasterID     string
	WorkerType   int64
	WorkerConfig []byte
}

func (d *TaskDispatcher) DispatchTask(
	ctx context.Context,
	args *DispatchTaskArgs,
	beforeWorkerRuns func(),
) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	requestID, err := d.preDispatchTaskWithRetry(ctx, args)
	if err != nil {
		return err
	}

	// The timer must be started before invoking ConfirmDispatchTask.
	beforeWorkerRuns()

	return d.confirmDispatchTask(ctx, requestID, args.WorkerID)
}

func (d *TaskDispatcher) preDispatchTaskWithRetry(
	ctx context.Context, args *DispatchTaskArgs,
) (requestID string, retErr error) {
	rl := rate.NewLimiter(rate.Every(preDispatchTaskRetryInterval), 1)
	for {
		select {
		case <-ctx.Done():
			return "", errors.Trace(ctx.Err())
		default:
		}

		// requestID is regenerated each time for tracing purpose.
		requestID := uuid.New().String()

		// The response is irrelevant because it is empty.
		_, err := d.client.Send(ctx, &ExecutorRequest{
			Cmd: CmdPreDispatchTask,
			Req: &pb.PreDispatchTaskRequest{
				TaskTypeId: args.WorkerType,
				TaskConfig: args.WorkerConfig,
				MasterId:   args.MasterID,
				WorkerId:   args.WorkerID,
				RequestId:  requestID,
			},
		})
		if err != nil {
			st := status.Convert(err)
			switch st.Code() {
			case codes.Aborted:
				// The business logic should be notified.
				return "", errors.Trace(err)
			case codes.AlreadyExists:
				// Since we are generating unique UUIDs, this should not happen.
				log.L().Panic("Unexpected error", zap.Error(err))
				panic("unreachable")
			default:
				log.L().Warn("PreDispatchTask encountered error, retrying", zap.Error(err))
			}

			if rlErr := rl.Wait(ctx); rlErr != nil {
				// The rate limiter only returns error when
				// the context is timing out.
				return "", errors.Trace(err)
			}

			continue
		}
		return requestID, nil
	}
}

func (d *TaskDispatcher) confirmDispatchTask(ctx context.Context, requestID string, workerID string) error {
	// The response is irrelevant because it is empty.
	_, err := d.client.Send(ctx, &ExecutorRequest{
		Cmd: CmdConfirmDispatchTask,
		Req: &pb.ConfirmDispatchTaskRequest{
			WorkerId:  workerID,
			RequestId: requestID,
		},
	})
	if err != nil {
		// The current implementation of the Executor does not support idempotency,
		// so we are not retrying.
		return errors.Trace(err)
	}
	return nil
}
