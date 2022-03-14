package model

import (
	"time"

	"github.com/hanfei1991/microcosm/model"
)

type LeaseType = string

const (
	LeaseTypeJob     = LeaseType("job")
	LeaseTypeWorker  = LeaseType("worker")
	LeaseTypeTimeout = LeaseType("timeout")
)

type (
	WorkerID   = string
	ResourceID = string
	JobID      = string
	ExecutorID = model.ExecutorID
)

type ResourceMeta struct {
	ID        ResourceID `json:"id"`
	LeaseType LeaseType  `json:"lease_type"`
	Job       JobID      `json:"job"`
	Worker    WorkerID   `json:"worker"`
	Executor  ExecutorID `json:"executor"`
}

type ResourceGCCountDown struct {
	ID           ResourceID `json:"id"`
	Job          JobID      `json:"job"`
	TargetGCTime time.Time  `json:"target_gc_time"`
}
