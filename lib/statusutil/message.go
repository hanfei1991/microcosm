package statusutil

import "fmt"

type WorkerStatusMessage[T status[T]] struct {
	Worker      WorkerID `json:"worker"`
	MasterEpoch Epoch    `json:"master-epoch"`
	Status      T        `json:"status"`
}

func WorkerStatusTopic(masterID MasterID) string {
	return fmt.Sprintf("worker-status-%s", masterID)
}
