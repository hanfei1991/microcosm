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
	var err error
	switch req.Cmd {
	case CmdSubmitSubJob:
		resp.Resp, err = c.client.SubmitSubJob(ctx, req.SubmitSubJob())
	}
	return resp, err
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
	CmdSubmitSubJob CmdType = 1 + iota
)

type ExecutorRequest struct {
	Cmd CmdType
	Req interface{}
}

func (e *ExecutorRequest) SubmitSubJob() *pb.SubmitSubJobRequest {
	return e.Req.(*pb.SubmitSubJobRequest)
}

type ExecutorResponse struct {
	Resp interface{}
}