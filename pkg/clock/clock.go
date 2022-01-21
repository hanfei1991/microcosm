package clock

import (
	"time"

	bclock "github.com/benbjohnson/clock"
	"github.com/gavv/monotime"
)

type (
	Timer         = bclock.Timer
	Ticker        = bclock.Ticker
	MonotonicTime time.Duration
)

var unixEpoch = time.Unix(0, 0)

type Clock interface {
	After(d time.Duration) <-chan time.Time
	AfterFunc(d time.Duration, f func()) *Timer
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
	Tick(d time.Duration) <-chan time.Time
	Ticker(d time.Duration) *Ticker
	Timer(d time.Duration) *Timer
	Mono() MonotonicTime
}

type withRealMono struct {
	bclock.Clock
}

func (r withRealMono) Mono() MonotonicTime {
	return MonotonicTime(monotime.Now())
}

type Mock struct {
	*bclock.Mock
}

func (r Mock) Mono() MonotonicTime {
	return MonotonicTime(r.Now().Sub(unixEpoch))
}

func New() Clock {
	return withRealMono{bclock.New()}
}

func NewMock() *Mock {
	return &Mock{bclock.NewMock()}
}

func (m MonotonicTime) Sub(other MonotonicTime) time.Duration {
	return time.Duration(m - other)
}

func MonoNow() MonotonicTime {
	return MonotonicTime(monotime.Now())
}
