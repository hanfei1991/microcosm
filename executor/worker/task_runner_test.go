package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	workerNum = 1000
)

func TestTaskRunnerBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := NewTaskRunner(2000, workerNum+1, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tr.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	var workers []*dummyWorker
	for i := 0; i < workerNum; i++ {
		worker := &dummyWorker{
			id: fmt.Sprintf("worker-%d", i),
		}
		workers = append(workers, worker)
		err := tr.AddTask(worker)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		t.Logf("taskNum %d", tr.Workload())
		return tr.Workload() == workerNum
	}, 1*time.Second, 10*time.Millisecond)

	for _, worker := range workers {
		worker.SetFinished()
	}

	require.Eventually(t, func() bool {
		return tr.Workload() == 0
	}, 1*time.Second, 100*time.Millisecond)

	cancel()
}
