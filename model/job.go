package model

import "github.com/hanfei1991/microcosom/pb"

type JobID  int32
type TaskID int32

type Job struct {
	ID    JobID 
	Tasks []*Task
}

func (j *Job) ToPB() (*pb.SubmitSubJobRequest) {
	req := &pb.SubmitSubJobRequest{
		JobId: int32(j.ID),
	}
	for _, t:= range j.Tasks {
		req.Tasks = append(req.Tasks, t.ToPB())
	}
	return req
}

type Task struct {
	ID TaskID 
	JobID JobID
	outputChannels []*Channel
	intputChannels []*Channel

	// TODO: operator or operator tree
	op Operator	
	Cost int
	PreferedLocation string

	ExecutorID ExecutorID
	Status   TaskStatus
}

type TaskStatus int
const (
	TaskScheduling TaskStatus = iota
	TaskPreparing
	TaskRunning
	TaskCanceling
	TaskClosed
)

func (t *Task) ToPB() *pb.TaskRequest {
	req := &pb.TaskRequest{
		Id : int32(t.ID),
		PlanDescription: string(t.op),
	}
	for _, c := range t.intputChannels {
		req.Inputs = append(req.Inputs, c.ToPB())
	}
	for _, c := range t.outputChannels {
		req.Outputs = append(req.Outputs, c.ToPB())
	}
	return req
}

type Channel struct {
	// Channel Type
	src *Task
	dst *Task
}

func (c *Channel) ToPB() *pb.Channel {
	return &pb.Channel{
		SrcId: int32(c.src.ID),
		DstId: int32(c.dst.ID),
	}
}

type Operator string 