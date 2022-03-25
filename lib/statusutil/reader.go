package statusutil

import (
	"github.com/hanfei1991/microcosm/pkg/containers"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
)

const (
	maxPendingStatusNum = 1024
)

//nolint:structcheck
type Reader[T status[T]] struct {
	q            containers.Queue[T]
	lastReceived T
}

func NewReader[T status[T]](init T) *Reader[T] {
	return &Reader[T]{
		q:            containers.NewDeque[T](),
		lastReceived: init,
	}
}

func (r *Reader[T]) Receive() (T, bool) {
	st, ok := r.q.Pop()
	if !ok {
		var noVal T
		return noVal, false
	}
	r.lastReceived = st
	return st, true
}

func (r *Reader[T]) OnAsynchronousNotification(newStatus T) error {
	if s := r.q.Size(); s > maxPendingStatusNum {
		return derror.ErrTooManyStatusUpdates.GenWithStackByArgs(s)
	}
	r.q.Add(newStatus)
	return nil
}

func (r *Reader[T]) Status() T {
	return r.lastReceived
}
