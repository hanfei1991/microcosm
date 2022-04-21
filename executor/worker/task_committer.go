package worker

import (
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/executor/worker/internal"
	"github.com/hanfei1991/microcosm/pkg/clock"
)

const runTTLCheckerInterval = 1 * time.Second

type requestID = string

type requestEntry struct {
	RequestID requestID
	ExpireAt  time.Time

	// task is a RunnableContainer, which contains
	// the task's submit time.
	task *internal.RunnableContainer
}

func (r *requestEntry) TaskID() internal.RunnableID {
	return r.task.ID()
}

// TaskCommitter is used to implement two-phase task dispatching.
type TaskCommitter struct {
	runner *TaskRunner

	mu               sync.Mutex
	pendingRequests  map[requestID]*requestEntry
	requestsByTaskID map[RunnableID]*requestEntry

	clock    clock.Clock
	wg       sync.WaitGroup
	cancelCh chan struct{}

	requestTTL time.Duration
}

func NewTaskCommitter(runner *TaskRunner, requestTTL time.Duration) *TaskCommitter {
	committer := &TaskCommitter{
		runner: runner,

		pendingRequests: make(map[requestID]*requestEntry),

		clock:      clock.New(),
		cancelCh:   make(chan struct{}),
		requestTTL: requestTTL,
	}

	committer.wg.Add(1)
	go func() {
		defer committer.wg.Done()
		committer.runTTLChecker()
	}()

	return committer
}

func (c *TaskCommitter) PreDispatchTask(rID requestID, task Runnable) (ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.pendingRequests[rID]; exists {
		// Duplicate requests are not allowed.
		// As we use UUIDs as request IDs, there should not be duplications.
		// The calling convention is that you should NOT retry the pre-dispatch API call.
		return false
	}

	// We need to overwrite stale request for the same workerID.
	if request, exists := c.requestsByTaskID[task.ID()]; exists {
		log.L().Info("There exists a previous request with the same worker ID, overwriting it.",
			zap.Any("request", request))

		c.removeRequestByID(request.RequestID)
	}

	// We use the current time as the submit time of the task.
	taskWithSubmitTime := internal.WrapRunnable(task, c.clock.Now())

	request := &requestEntry{
		RequestID: rID,
		ExpireAt:  c.clock.Now().Add(c.requestTTL),
		task:      taskWithSubmitTime,
	}
	c.pendingRequests[rID] = request
	c.requestsByTaskID[task.ID()] = request

	return true
}

func (c *TaskCommitter) ConfirmDispatchTask(rID requestID, taskID RunnableID) (ok bool, retErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	request, ok := c.pendingRequests[rID]
	if !ok {
		log.L().Warn("ConfirmDispatchTask: request not found",
			zap.String("request-id", rID),
			zap.String("task-id", taskID))
		return false, nil
	}

	c.removeRequestByID(rID)

	if err := c.runner.AddTask(request.task); err != nil {
		return false, err
	}

	return true, nil
}

func (c *TaskCommitter) Close() {
	close(c.cancelCh)
	c.wg.Wait()
}

func (c *TaskCommitter) runTTLChecker() {
	ticker := c.clock.Ticker(runTTLCheckerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.cancelCh:
			return
		case <-ticker.C:
		}

		c.checkTTLOnce()
	}
}

func (c *TaskCommitter) checkTTLOnce() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for rID, request := range c.pendingRequests {
		expireAt := request.ExpireAt
		if c.clock.Now().After(expireAt) {
			log.L().Info("Pending request has expired.",
				zap.Any("request", request),
				zap.String("task-id", request.TaskID()))
			c.removeRequestByID(rID)
		}
	}
}

// removeRequestByID should be called with c.mu taken.
func (c *TaskCommitter) removeRequestByID(id requestID) {
	request, ok := c.pendingRequests[id]
	if !ok {
		log.L().Panic("Unreachable", zap.String("request-id", id))
	}

	taskID := request.TaskID()
	if _, ok := c.requestsByTaskID[taskID]; !ok {
		log.L().Panic("Unreachable",
			zap.Any("request", request),
			zap.String("task-id", taskID))
	}

	delete(c.pendingRequests, id)
	delete(c.requestsByTaskID, taskID)
}
