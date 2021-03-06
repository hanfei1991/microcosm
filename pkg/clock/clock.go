package clock

import (
	"time"

	bclock "github.com/benbjohnson/clock"
	"github.com/gavv/monotime"
)

type (
	// MonotonicTime alias to time.Duration
	MonotonicTime time.Duration
)

var unixEpoch = time.Unix(0, 0)

// Clock defines an interface that combines github.com/benbjohnson/clock.Clock
// and a Mono methods that can return a monotonic time duration
type Clock interface {
	bclock.Clock
	Mono() MonotonicTime
}

type withRealMono struct {
	bclock.Clock
}

func (r withRealMono) Mono() MonotonicTime {
	return MonotonicTime(monotime.Now())
}

// Mock is a mock struct that implements Clock interface
type Mock struct {
	*bclock.Mock
}

// Mono implements Clock.Mono
func (r Mock) Mono() MonotonicTime {
	return MonotonicTime(r.Now().Sub(unixEpoch))
}

// New creates a new withRealMono instance, which implements Clock interface
func New() Clock {
	return withRealMono{bclock.New()}
}

// NewMock creates a new Mock instance
func NewMock() *Mock {
	return &Mock{bclock.NewMock()}
}

// Sub returns time difference between two MonotonicTime
func (m MonotonicTime) Sub(other MonotonicTime) time.Duration {
	return time.Duration(m - other)
}

// MonoNow returns the MonotonicTime of current
func MonoNow() MonotonicTime {
	return MonotonicTime(monotime.Now())
}

// ToMono converts time.Time to MonotonicTime
func ToMono(t time.Time) MonotonicTime {
	return MonotonicTime(t.Sub(unixEpoch))
}
