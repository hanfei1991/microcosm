package model

import (
	"time"

	"github.com/hanfei1991/microcosm/model"
)

type KeepAliveStrategy = string

const (
	KeepAliveByJob               = KeepAliveStrategy("job")
	KeepAliveByWorker            = KeepAliveStrategy("worker")
	KeepAliveByWorkerWithTimeout = KeepAliveStrategy("timeout")
)

type WorkerID = string
type ResourceID = string
type JobID = string
type ExecutorID = model.ExecutorID

type ResourceMeta struct {
	ID        ResourceID `json:"id"`
	Persisted bool       `json:"persisted"`
	Job       JobID      `json:"job"`
	Worker    WorkerID   `json:"worker"`
	Executor  ExecutorID `json:"executor"`
}

type ResourceGCCountDown struct {
	ID           ResourceID `json:"id"`
	Job          JobID      `json:"job"`
	TargetGCTime time.Time  `json:"target_gc_time"`
}
