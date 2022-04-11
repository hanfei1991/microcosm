package master

import libModel "github.com/hanfei1991/microcosm/lib/model"

type masterEventType int32

const (
	workerOnlineEvent = masterEventType(iota + 1)
	workerOfflineEvent
	workerStatusUpdatedEvent
	workerDispatched
)

type masterEvent struct {
	Tp         masterEventType
	Handle     WorkerHandle
	WorkerID   libModel.WorkerID
	Err        error
	beforeHook func()
}
