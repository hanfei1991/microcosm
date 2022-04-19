package errctx

import (
	"context"
	"sync"
)

type errCtx struct {
	context.Context
	center *ErrCenter

	once   sync.Once
	doneCh <-chan struct{}
}

func newErrCtx(parent context.Context, center *ErrCenter) *errCtx {
	return &errCtx{
		Context: parent,
		center:  center,
	}
}

func (c *errCtx) Done() <-chan struct{} {
	c.once.Do(func() {
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
	})
	return c.doneCh
}

func (c *errCtx) Err() error {
	if err := c.center.CheckError(); err != nil {
		return err
	}

	return c.Context.Err()
}
