package model

import "github.com/hanfei1991/microcosm/pb"

type (
	// ID is the global identified number for jobs and tasks.
	// For job, the id number is less than int32
	// For task, the id number is a int64-number (job id, task id)
	// that job id is the higher 32-bit and task id is the lower 32-bit.
	ID           int64
	WorkloadType int32
	TaskStatus   int32
)

const (
	Benchmark WorkloadType = iota
	DM
	CDC
)

const (
	Init TaskStatus = iota
	Serving
	Pauseed
	Stopped
)

type JobMaster struct {
	ID          ID           `json:"id"`
	Tp          WorkloadType `json:"type"`
	Config      []byte       `json:"config"`
	MasterAddrs []string     `json:"masters"`
}

type Task struct {
	// FlowID is unique for a same dataflow, passed from submitted job
	FlowID string `json:"flow_id"`

	ID      ID   `json:"id"`
	Outputs []ID `json:"outputs"`
	Inputs  []ID `json:"inputs"`

	// TODO: operator or operator tree
	OpTp              OperatorType `json:"type"`
	Op                Operator     `json:"op"`
	Cost              int          `json:"cost"`
	PreferredLocation string       `json:"location"`

	Exec   ExecutorID `json:"exec"`
	Status TaskStatus
}

func (t *Task) ToPB() *pb.TaskRequest {
	req := &pb.TaskRequest{
		Id:   int64(t.ID),
		Op:   t.Op,
		OpTp: int32(t.OpTp),
	}
	for _, c := range t.Inputs {
		req.Inputs = append(req.Inputs, int64(c))
	}
	for _, c := range t.Outputs {
		req.Outputs = append(req.Outputs, int64(c))
	}
	return req
}

// ToScheduleTaskPB converts a task to a schedule task request
func (t *Task) ToScheduleTaskPB() *pb.ScheduleTask {
	req := &pb.ScheduleTask{
		Task:              t.ToPB(),
		Cost:              int64(t.Cost),
		PreferredLocation: t.PreferredLocation,
	}
	return req
}
