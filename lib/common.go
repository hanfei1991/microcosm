package lib

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/lib/master"
	"github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	WorkerType   = model.WorkerType
	WorkerConfig = interface{}
)

const (
	JobManager = model.WorkerType(iota + 1)
	// job master
	CvsJobMaster
	FakeJobMaster
	DMJobMaster
	CdcJobMaster
	// task
	CvsTask
	FakeTask
	DmTask
	CdcTask
	// worker
	WorkerDMDump
	WorkerDMLoad
	WorkerDMSync
)

type TimeoutConfig struct {
	workerTimeoutDuration            time.Duration
	workerTimeoutGracefulDuration    time.Duration
	workerHeartbeatInterval          time.Duration
	workerReportStatusInterval       time.Duration
	masterHeartbeatCheckLoopInterval time.Duration
}

var defaultTimeoutConfig TimeoutConfig = TimeoutConfig{
	workerTimeoutDuration:            time.Second * 15,
	workerTimeoutGracefulDuration:    time.Second * 5,
	workerHeartbeatInterval:          time.Second * 3,
	workerReportStatusInterval:       time.Second * 3,
	masterHeartbeatCheckLoopInterval: time.Second * 1,
}.Adjust()

// Adjust validates the TimeoutConfig and adjusts it
func (config TimeoutConfig) Adjust() TimeoutConfig {
	var tc TimeoutConfig = config
	// worker timeout duration must be 2 times larger than worker heartbeat interval
	if tc.workerTimeoutDuration < 2*tc.workerHeartbeatInterval+time.Second*3 {
		tc.workerTimeoutDuration = 2*tc.workerHeartbeatInterval + time.Second*3
	}
	return tc
}

func WorkerStatusChangeRequestTopic(masterID model.MasterID, workerID model.WorkerID) p2p.Topic {
	return fmt.Sprintf("worker-status-change-req-%s-%s", masterID, workerID)
}

type StatusChangeRequest struct {
	SendTime     clock.MonotonicTime    `json:"send-time"`
	FromMasterID model.MasterID         `json:"from-master-id"`
	Epoch        model.Epoch            `json:"epoch"`
	ExpectState  model.WorkerStatusCode `json:"expect-state"`
}

type WorkerMetaKVData struct {
	MasterID   Master                 `json:"id"`
	NodeID     p2p.NodeID             `json:"node-id"`
	StatusCode model.WorkerStatusCode `json:"status-code"`
	Message    string                 `json:"message"`
}

type MasterFailoverReasonCode int32

const (
	MasterTimedOut = MasterFailoverReasonCode(iota + 1)
	MasterReportedError
)

type MasterFailoverReason struct {
	Code         MasterFailoverReasonCode
	ErrorMessage string
}

type WorkerHandle = master.WorkerHandle

// nolint:revive
var StopAfterTick = errors.New("stop after tick")
