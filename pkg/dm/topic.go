package dm

import (
	"fmt"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// Message use for asynchronous message.
// It use json format to transfer, all the fields should be public.
type Message interface{}

// MessageWithID use for synchronous request/response message.
type MessageWithID struct {
	ID      uint64
	Message Message
}

// Request alias to Message
type Request Message

// Response alias to Message
type Response Message

// OperateTaskMessageTopic is topic constructor for operate task message
func OperateTaskMessageTopic(masterID libModel.MasterID, taskID string) p2p.Topic {
	return fmt.Sprintf("operate-task-message-%s-%s", masterID, taskID)
}

// OperateTaskMessage is operate task message
type OperateTaskMessage struct {
	TaskID string
	Stage  metadata.TaskStage
}

// QueryStatusRequestTopic is topic constructor for query status request
func QueryStatusRequestTopic(masterID libModel.MasterID, taskID string) p2p.Topic {
	return fmt.Sprintf("query-status-request-%s-%s", masterID, taskID)
}

// QueryStatusResponseTopic is topic constructor for query status response
func QueryStatusResponseTopic(workerID libModel.WorkerID, taskID string) p2p.Topic {
	return fmt.Sprintf("query-status-response-%s-%s", workerID, taskID)
}

// QueryStatusRequest is query status request
type QueryStatusRequest struct {
	Task string
}

// QueryStatusResponse is query status response
type QueryStatusResponse struct {
	ErrorMsg   string
	TaskStatus runtime.TaskStatus
}
