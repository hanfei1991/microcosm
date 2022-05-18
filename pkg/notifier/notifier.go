package notifier

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/atomic"

	"github.com/hanfei1991/microcosm/pkg/containers"
)

type receiverID = int64

// Notifier is the sending endpoint of a single-producer-multiple-consumer
// notification mechanism.
type Notifier[T any] struct {
	receivers sync.Map // receiverID -> *Receiver[T]
	nextID    atomic.Int64

	queue *containers.SliceQueue[T]

	closeCh       chan struct{}
	synchronizeCh chan struct{}
	closeOnce     sync.Once
}

// Receiver is the receiving endpoint of a single-producer-multiple-consumer
// notification mechanism.
type Receiver[T any] struct {
	id receiverID
	C  chan T

	closeOnce sync.Once
	closed    atomic.Bool

	notifier *Notifier[T]
}

func (r *Receiver[T]) close() {
	r.closed.Store(true)
	r.closeOnce.Do(
		func() {
			close(r.C)
		})
}

// Close closes the receiver
func (r *Receiver[T]) Close() {
	r.closed.Store(true)
	select {
	case <-r.notifier.synchronizeCh:
	case <-r.notifier.closeCh:
	}

	r.closeOnce.Do(
		func() {
			close(r.C)
		})
}

// NewNotifier creates a new Notifier.
func NewNotifier[T any]() *Notifier[T] {
	ret := &Notifier[T]{
		receivers:     sync.Map{},
		queue:         containers.NewSliceQueue[T](),
		closeCh:       make(chan struct{}),
		synchronizeCh: make(chan struct{}),
	}

	go ret.run()
	return ret
}

// NewReceiver creates a new Receiver associated with
// the given Notifier.
func (n *Notifier[T]) NewReceiver() *Receiver[T] {
	ch := make(chan T, 16)
	receiver := &Receiver[T]{
		id:       n.nextID.Add(1),
		C:        ch,
		notifier: n,
	}

	n.receivers.Store(receiver.id, receiver)
	return receiver
}

// Notify sends a new notification event.
func (n *Notifier[T]) Notify(event T) {
	n.queue.Add(event)
}

// Close closes the notifier.
func (n *Notifier[T]) Close() {
	n.closeOnce.Do(func() {
		close(n.closeCh)

		var receivers []*Receiver[T]
		n.receivers.Range(func(_, value any) bool {
			receiver := value.(*Receiver[T])
			receivers = append(receivers, receiver)
			return true
		})

		<-n.synchronizeCh

		for _, receiver := range receivers {
			receiver.close()
		}
	})
}

// Flush flushes all pending notifications.
func (n *Notifier[T]) Flush(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-n.synchronizeCh:
		}

		if n.queue.Size() == 0 {
			return nil
		}
	}
}

func (n *Notifier[T]) run() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	defer func() {
		close(n.synchronizeCh)
	}()

	for {
		select {
		case <-n.closeCh:
			return
		case n.synchronizeCh <- struct{}{}:
			// no-op here. Just a synchronization barrier.
		case <-n.queue.C:
		Inner:
			for {
				event, ok := n.queue.Pop()
				if !ok {
					break Inner
				}

				n.receivers.Range(func(_, value any) bool {
					receiver := value.(*Receiver[T])

					if receiver.closed.Load() {
						return true
					}

					select {
					case <-n.closeCh:
						return false
					case receiver.C <- event:
						// send the event to the receiver.
					}
					return true
				})

				select {
				case <-n.closeCh:
					return
				default:
				}
			}
		}
	}
}
