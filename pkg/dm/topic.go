package dm

import (
	"fmt"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// Message use for asynchronous message.
type Message interface{}

// MessageWithID use for synchronous request/response message.
type MessageWithID struct {
	ID      uint64
	Message interface{}
}

type Request Message

type Response Message

func OperateTaskMessageTopic(masterID lib.MasterID, taskID string) p2p.Topic {
	return fmt.Sprintf("operate-task-message-%s-%s", masterID, taskID)
}

type OperateTaskMessage struct {
	TaskID string
	Stage  metadata.TaskStage
}
