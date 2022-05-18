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

	closed        atomic.Bool
	closeCh       chan struct{}
	synchronizeCh chan struct{}

	wg sync.WaitGroup
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

// Close closes the receiver
func (r *Receiver[T]) Close() {
	r.closeOnce.Do(
		func() {
			r.closed.Store(true)
			<-r.notifier.synchronizeCh
			close(r.C)
			r.notifier.receivers.Delete(r.id)
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

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.run()
	}()
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
	if n.closed.Swap(true) {
		// Ensures idempotency of closing once.
		return
	}

	close(n.closeCh)
	n.wg.Wait()

	n.receivers.Range(func(_, value any) bool {
		receiver := value.(*Receiver[T])
		receiver.Close()
		return true
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
