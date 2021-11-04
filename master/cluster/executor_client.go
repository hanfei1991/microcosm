package cluster

import (
	"context"
	"errors"

	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"google.golang.org/grpc"
)

type ExecutorClient interface {
	Send(context.Context, model.ExecutorID, *ExecutorRequest) (*ExecutorResponse, error)
}

type executorClient struct {
	conn *grpc.ClientConn
	client pb.ExecutorClient
}

func (c *executorClient) send(ctx context.Context, req *ExecutorRequest) (*ExecutorResponse, error) {
	resp := &ExecutorResponse{}
	switch req.Cmd {
	case CmdDispatchSubJob:
		c.client.SubmitSubJob(ctx, req.)
	}

}

func newExecutorClient(addr string) (*executorClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.New("cannot build conn")
	}
	return &executorClient{
		conn: conn,
	}, nil
}

type CmdType uint16

const (
	CmdDispatchSubJob CmdType = 1 + iota
)

type ExecutorRequest struct {
	Cmd CmdType
	Req interface{}
}

func (e *ExecutorRequest) DispatchSubJob() *pb.DispatchSubJob {

}

type ExecutorResponse struct {
	resp interface{}
}