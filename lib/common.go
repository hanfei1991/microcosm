package lib

import (
	"fmt"
	"time"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	MasterID     string
	WorkerID     string
	WorkerStatus int32

	epoch = int64
)

const (
	WorkerStatusNormal = WorkerStatus(iota + 1)
	WorkerStatusInit
	WorkerStatusError
)

func workerToMasterHeartbeatTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-%s", string(masterID))
}

func masterToWorkerHeartbeatTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-%s-resp", string(masterID))
}

type workerToMasterHeartbeatMessage struct {
	SendTime     time.Time    `json:"send-time"`
	Status       WorkerStatus `json:"status"`
	FromWorkerID WorkerID     `json:"from-id"`
	Epoch        epoch        `json:"epoch"`
}

type masterToWorkerHeartbeatMessage struct {
	SendTime     time.Time `json:"send-time"`
	ReplyTime    time.Time `json:"reply-time"`
	FromMasterID MasterID  `json:"from-id"`
	Epoch        epoch     `json:"epoch"`
}
