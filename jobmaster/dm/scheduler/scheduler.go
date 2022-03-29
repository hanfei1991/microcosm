package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
)

const (
	checkInterval = 50 * time.Millisecond
)

type Scheduler interface {
	Schedule(ctx context.Context) error
}

// DefaultScheduler defines the template method to schedule periodically.
type DefaultScheduler struct {
	Scheduler

	mu            sync.RWMutex
	lastCheckTime time.Time
	nextCheckTime time.Time

	normalInterval time.Duration
	errorInterval  time.Duration
}

func NewDefaultScheduler(normalInterval, errorInterval time.Duration) *DefaultScheduler {
	defaultScheduler := &DefaultScheduler{
		normalInterval: normalInterval,
		errorInterval:  errorInterval,
	}
	return defaultScheduler
}

func (s *DefaultScheduler) setNextCheckTime(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if (t.After(s.lastCheckTime) && t.Before(s.nextCheckTime)) || s.lastCheckTime.Equal(s.nextCheckTime) {
		s.nextCheckTime = t
	}
}

func (s *DefaultScheduler) getNextCheckTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextCheckTime
}

func (s *DefaultScheduler) advanceCheckTime() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.lastCheckTime = now
	s.nextCheckTime = now
}

func (s *DefaultScheduler) Trigger(delay time.Duration) {
	t := time.Now().Add(delay)
	s.setNextCheckTime(t)
}

func (s *DefaultScheduler) Run(ctx context.Context) {
	// trigger the first check
	s.Trigger(s.normalInterval)
	ticker := time.NewTicker(checkInterval)

	for {
		select {
		case <-ctx.Done():
			log.L().Info("exit scheduler run")
			return
		case <-ticker.C:
			if time.Now().Before(s.getNextCheckTime()) {
				continue
			}
		}
		s.advanceCheckTime()

		if err := s.Schedule(ctx); err != nil {
			// TODO: add backoff strategy
			s.Trigger(s.errorInterval)
		} else {
			s.Trigger(s.normalInterval)
		}
	}
}
