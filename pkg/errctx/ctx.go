package errctx

import (
	"context"
	"sync"

	"go.uber.org/atomic"
)

type errCtx struct {
	context.Context
	center *ErrCenter

	hasDoneCh atomic.Bool
	mu        sync.Mutex
	doneCh    <-chan struct{}
}

func newErrCtx(parent context.Context, center *ErrCenter) *errCtx {
	return &errCtx{
		Context: parent,
		center:  center,
	}
}

func (c *errCtx) Done() <-chan struct{} {
	if c.hasDoneCh.Load() {
		// Fast path. No lock contention.
		return c.doneCh
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.hasDoneCh.Load() {
		// Re-check under lock. In case there was
		// a race with another goroutine.
		return c.doneCh
	}

	doneCh := make(chan struct{})

	go func() {
		select {
		case <-c.center.doneCh:
		case <-c.Context.Done():
		}

		close(doneCh)
	}()

	// There is no data race here because
	// c.doneCh is read from only if c.hasDoneCh
	// has true.
	c.doneCh = doneCh
	c.hasDoneCh.Store(true)

	return doneCh
}

func (c *errCtx) Err() error {
	if c.center.hasErr.Load() {
		return c.center.errVal.Load()
	}

	return c.Context.Err()
}
