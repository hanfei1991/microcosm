package autoid

import (
	"sync"

	"github.com/google/uuid"
)

type IDAllocator struct {
	sync.Mutex
	internalID int64
	jobID      int64
}

func NewIDAllocator(jobID int64) *IDAllocator {
	return &IDAllocator{
		jobID: jobID << 32,
	}
}

func (a *IDAllocator) AllocID() int64 {
	a.Lock()
	defer a.Unlock()
	a.internalID++
	return a.internalID + a.jobID
}

type UUIDAllocator struct{}

func NewUUIDAllocator() *UUIDAllocator {
	return new(UUIDAllocator)
}

func (a *UUIDAllocator) AllocID() string {
	return uuid.New().String()
}
