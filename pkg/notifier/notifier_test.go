package notifier

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotifierBasics(t *testing.T) {
	n := NewNotifier[int]()
	defer n.Close()

	const (
		numReceivers = 10
		numEvents    = 100000
		finEv        = math.MaxInt
	)
	var wg sync.WaitGroup

	for i := 0; i < numReceivers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			r := n.NewReceiver()
			defer r.Close()

			var ev, lastEv int
			for {
				select {
				case ev = <-r.C:
				}

				if ev == finEv {
					return
				}

				if lastEv != 0 {
					require.Equal(t, lastEv+1, ev)
				}
				lastEv = ev
			}
		}()
	}

	for i := 1; i <= numEvents; i++ {
		n.Notify(i)
	}

	n.Notify(finEv)
	err := n.Flush(context.Background())
	require.NoError(t, err)

	wg.Wait()
}
