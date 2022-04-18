package errctx

import (
	"context"

	"go.uber.org/atomic"
)

type ErrCenter struct {
	hasErr atomic.Bool
	errVal atomic.Error
	doneCh chan struct{}
}

func NewErrCenter() *ErrCenter {
	return &ErrCenter{
		doneCh: make(chan struct{}),
	}
}

func (c *ErrCenter) OnError(err error) {
	if err == nil {
		return
	}
	if c.hasErr.Swap(true) {
		// OnError is no-op after the first call with
		// a non-nil error.
		return
	}
	c.errVal.Store(err)
	close(c.doneCh)
}

func (c *ErrCenter) CheckError() error {
	return c.errVal.Load()
}

func (c *ErrCenter) DeriveContext(ctx context.Context) context.Context {
	return newErrCtx(ctx, c)
}
