package model

import (
	"encoding/json"

	"github.com/hanfei1991/microcosm/pkg/adapter"
)

// NodeType is the node type, could be either server master or executor
type NodeType int

const (
	NodeTypeServerMaster NodeType = iota + 1
	NodeTypeExecutor
)

// RescUnit is the min unit of resource that we count.
type RescUnit int

// DeployNodeID means the identify of a node
type DeployNodeID string

type ExecutorID = DeployNodeID

// NodeInfo describes deployment node information, the node could be server master.
// or executor.
type NodeInfo struct {
	Type NodeType     `json:"type"`
	ID   DeployNodeID `json:"id"`
	Addr string       `json:"addr"`

	// The capability of executor, including
	// 1. cpu (goroutines)
	// 2. memory
	// 3. disk cap
	// TODO: So we should enrich the cap dimensions in the future.
	Capability int `json:"cap"`
}

func (e *NodeInfo) EtcdKey() string {
	return adapter.NodeInfoKeyAdapter.Encode(string(e.ID))
}

type ExecutorStatus int32

const (
	Initing ExecutorStatus = iota
	Running
	Disconnected
	Tombstone
	Busy
)

func (e *NodeInfo) ToJSON() (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
