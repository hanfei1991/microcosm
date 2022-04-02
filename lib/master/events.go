package master

type masterEventType int32

const (
	workerOnlineEvent = masterEventType(iota + 1)
	workerOfflineEvent
	workerStatusUpdatedEvent
)

type masterEvent struct {
	Tp     masterEventType
	Handle WorkerHandle
}
