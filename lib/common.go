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

	epoch         = int64
	monotonicTime = uint64
)

const (
	WorkerStatusNormal = WorkerStatus(iota + 1)
	WorkerStatusInit
	WorkerStatusError
)

const (
	// If no heartbeat response is received for workerTimeoutDuration,
	// a worker will commit suicide.
	workerTimeoutDuration = time.Second * 15

	// If no heartbeat is received for workerTimeoutDuration + workerTimeoutGracefulDuration,
	// the master will consider a worker dead.
	workerTimeoutGracefulDuration = time.Second * 5
)

func HeartbeatPingTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-ping-%s", string(masterID))
}

func HeartbeatPongTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-pong-%s", string(masterID))
}

type HeartbeatPingMessage struct {
	SendTime     monotonicTime `json:"send-time"`
	Status       WorkerStatus  `json:"status"`
	FromWorkerID WorkerID      `json:"from-id"`
	Epoch        epoch         `json:"epoch"`
}

type HeartbeatPongMessage struct {
	SendTime  monotonicTime `json:"send-time"`
	ReplyTime time.Time     `json:"reply-time"`
	Epoch     epoch         `json:"epoch"`
}

type MasterMetaKVData struct {
	ID     MasterID   `json:"id"`
	Addr   string     `json:"addr"`
	NodeID p2p.NodeID `json:"node-id"`
	Epoch  epoch      `json:"epoch"`

	// Ext holds business-specific data
	Ext    interface{} `json:"ext"`
}

type WorkerInfo struct {
	ID     WorkerID
	Addr   string
	NodeID p2p.NodeID

	// fields for internal use by the Master.
	lastHeartBeatReceiveTime time.Time
	lastHeartBeatSendTime    monotonicTime
	hasPendingHeartbeat      bool
}
