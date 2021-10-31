package model

import "encoding/json"

type ExecutorStatus int

const (
	// The executor just start.
	ExecutorBootstrap ExecutorStatus = iota
	// The executor is running.
	ExecutorRunning
	// The executor is busy.
	ExecutorBusy 
	// The executor has been closed.
	ExecutorClosed
)

type ExecutorID int32

// ExecutorInfo describes an Executor.
type ExecutorInfo struct {
	ID 			      ExecutorID `json:"id"`
	Addr 			  string  `json:"addr"`
	// The capability of executor, including 
	// 1. cpu (goroutines)
	// 2. memory
	// 3. disk cap
	// TODO: So we should enrich the cap dimensions in the future.
	Capability        int     `json:"cap"`
	// What kind of information do we need?
	//LastHeartbeatTime int64    
}

type JobType int
const (
	JobDM JobType = iota
	JobCDC
	JobBenchmark
)


type JobInfo struct {
	Type   JobType  `json:"type"`
	Config string  `json:"config"` // describes a cdc/dm or other type of job.
	UserName  string  `json:"user"` // reserved field
	// CostQuota int // indicates how much resource this job will use.
}

//type TaskInfo struct {
//
//}

func (i ExecutorInfo) ToJSON() (string, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return string(data), nil
}