package manager

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/clock"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
)

type gcRunnerTestHelper struct {
	Runner *DefaultGCRunner
	Meta   pkgOrm.Client
	Clock  *clock.Mock

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	errCh  chan error

	gcRequestCh chan *resModel.ResourceMeta
}

func newGCRunnerTestHelper() *gcRunnerTestHelper {
	meta, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}

	reqCh := make(chan *resModel.ResourceMeta, 16)
	mockHandler := func(ctx context.Context, meta *resModel.ResourceMeta) error {
		select {
		case <-ctx.Done():
			return errors.Trace(err)
		case reqCh <- meta:
		}
		return nil
	}
	runner := NewGCRunner(meta, map[resModel.ResourceType]gcHandlerFunc{"local": mockHandler})
	clk := clock.NewMock()
	runner.clock = clk
	ctx, cancel := context.WithCancel(context.Background())

	return &gcRunnerTestHelper{
		Runner: runner,
		Meta:   meta,
		Clock:  clk,

		ctx:         ctx,
		cancel:      cancel,
		errCh:       make(chan error, 1),
		gcRequestCh: reqCh,
	}
}

func (h *gcRunnerTestHelper) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()

		h.errCh <- h.Runner.Run(h.ctx)
	}()
}

func (h *gcRunnerTestHelper) Close() {
	h.cancel()
	h.wg.Wait()
}

func (h *gcRunnerTestHelper) WaitGC(t *testing.T) (meta *resModel.ResourceMeta) {
	select {
	case <-time.After(2 * time.Second):
		t.FailNow()
	case meta = <-h.gcRequestCh:
	}
	return
}

func TestGCRunnerNotify(t *testing.T) {
	helper := newGCRunnerTestHelper()

	err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:        "/local/resource-1",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
		GCPending: true,
	})
	require.NoError(t, err)

	helper.Start()
	// Note that since we are not advancing the clock,
	// GC can only be triggered by calling Notify.
	helper.Runner.Notify()

	gcRes := helper.WaitGC(t)
	require.Equal(t, "/local/resource-1", gcRes.ID)

	helper.Close()
}

func TestGCRunnerTicker(t *testing.T) {
	helper := newGCRunnerTestHelper()

	err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
		ID:        "/local/resource-1",
		Job:       "job-1",
		Worker:    "worker-1",
		Executor:  "executor-1",
		GCPending: true,
	})
	require.NoError(t, err)

	helper.Start()
	time.Sleep(10 * time.Millisecond)
	helper.Clock.Add(10 * time.Second)
	helper.Clock.Add(10 * time.Second)

	gcRes := helper.WaitGC(t)
	require.Equal(t, "/local/resource-1", gcRes.ID)

	helper.Close()
}

func TestGCRunnerMultiple(t *testing.T) {
	helper := newGCRunnerTestHelper()

	const numResources = 1000
	for i := 0; i < numResources; i++ {
		err := helper.Meta.CreateResource(context.Background(), &resModel.ResourceMeta{
			ID:        fmt.Sprintf("/local/resource-%d", i),
			Job:       "job-1",
			Worker:    "worker-1",
			Executor:  "executor-1",
			GCPending: i%2 == 0, // marks half the resources as needing GC.
		})
		require.NoError(t, err)
	}

	helper.Start()

	alreadyGCedSet := make(map[resModel.ResourceID]struct{})
loop:
	for {
		select {
		case meta := <-helper.gcRequestCh:
			_, exists := alreadyGCedSet[meta.ID]
			require.False(t, exists)
			alreadyGCedSet[meta.ID] = struct{}{}

			if len(alreadyGCedSet) == 500 {
				break loop
			}
		default:
		}

		helper.Runner.Notify()
	}

	helper.Close()
}
