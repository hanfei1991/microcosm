package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestDefaultScheduler(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dummyScheduler := NewDummyScheduler()
	var wg sync.WaitGroup

	wg.Add(1)
	// run task manager
	go func() {
		defer wg.Done()
		dummyScheduler.Run(ctx)
	}()

	scheduleError := errors.New("schedule error")
	dummyScheduler.SetResult([]error{scheduleError, scheduleError, nil})

	// trigger when start
	dummyScheduler.Trigger(0)
	require.Eventually(t, func() bool {
		dummyScheduler.Lock()
		defer dummyScheduler.Unlock()
		return len(dummyScheduler.results) == 0
	}, 5*time.Second, 100*time.Millisecond)

	// mock manual trigger
	dummyScheduler.SetResult([]error{scheduleError, scheduleError, nil})
	dummyScheduler.Trigger(0)
	require.Eventually(t, func() bool {
		dummyScheduler.Lock()
		defer dummyScheduler.Unlock()
		return len(dummyScheduler.results) == 0
	}, 5*time.Second, 100*time.Millisecond)

	// mock trigger with delay
	dummyScheduler.SetResult([]error{nil})
	dummyScheduler.Trigger(time.Second)
	require.Eventually(t, func() bool {
		dummyScheduler.Lock()
		defer dummyScheduler.Unlock()
		return len(dummyScheduler.results) == 0
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}

type DummyScheduler struct {
	*DefaultScheduler

	sync.Mutex
	results []error
}

func NewDummyScheduler() *DummyScheduler {
	dummyScheduler := &DummyScheduler{
		DefaultScheduler: NewDefaultScheduler(time.Hour, 100*time.Millisecond),
	}
	dummyScheduler.DefaultScheduler.Scheduler = dummyScheduler
	return dummyScheduler
}

func (ds *DummyScheduler) SetResult(results []error) {
	ds.Lock()
	defer ds.Unlock()
	ds.results = append(ds.results, results...)
}

func (ds *DummyScheduler) Schedule(ctx context.Context) error {
	ds.Lock()
	defer ds.Unlock()
	if len(ds.results) == 0 {
		panic("no result in dummy scheduler")
	}
	result := ds.results[0]
	ds.results = ds.results[1:]
	return result
}
