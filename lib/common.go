package lib

import (
	"fmt"
	"github.com/hanfei1991/microcosm/model"
	"time"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	MasterID         string
	WorkerID         string
	WorkerStatusCode int32
	WorkerType       int64

	epoch         = int64
	monotonicTime = uint64
)

const (
	WorkerStatusNormal = WorkerStatusCode(iota + 1)
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

type WorkerStatus struct {
	Code         WorkerStatusCode `json:"code"`
	ErrorMessage string           `json:"error-message"`
	Ext          interface{}      `json:"ext"`
}

func HeartbeatPingTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-ping-%s", string(masterID))
}

func HeartbeatPongTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("heartbeat-pong-%s", string(masterID))
}

func WorkloadReportTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("workload-report-%s", masterID)
}

func StatusUpdateTopic(masterID MasterID) p2p.Topic {
	return fmt.Sprintf("status-update-%s", masterID)
}

type HeartbeatPingMessage struct {
	SendTime     monotonicTime `json:"send-time"`
	FromWorkerID WorkerID      `json:"from-id"`
	Epoch        epoch         `json:"epoch"`
}

type HeartbeatPongMessage struct {
	SendTime  monotonicTime `json:"send-time"`
	ReplyTime time.Time     `json:"reply-time"`
	Epoch     epoch         `json:"epoch"`
}

type StatusUpdateMessage struct {
	WorkerID WorkerID     `json:"worker-id"`
	Status   WorkerStatus `json:"status"`
}

type WorkloadReportMessage struct {
	WorkerID WorkerID       `json:"worker-id"`
	Workload model.RescUnit `json:"workload"`
}

type MasterMetaKVData struct {
	ID     MasterID   `json:"id"`
	Addr   string     `json:"addr"`
	NodeID p2p.NodeID `json:"node-id"`
	Epoch  epoch      `json:"epoch"`

	// Ext holds business-specific data
	Ext interface{} `json:"ext"`
}

type WorkerInfo struct {
	ID     WorkerID
	NodeID p2p.NodeID

	// fields for internal use by the Master.
	lastHeartBeatReceiveTime time.Time
	lastHeartBeatSendTime    monotonicTime
	hasPendingHeartbeat      bool

	status   WorkerStatus
	workload model.RescUnit
}
