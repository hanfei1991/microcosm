package cluster

import (
	"context"
	"errors"

	"github.com/hanfei1991/microcosom/model"
	"google.golang.org/grpc"
)

type ExecutorClient interface {
	Send(context.Context, model.ExecutorID, *ExecutorRequest) (*ExecutorResponse, error)
}

type executorClient struct {
	conn *grpc.ClientConn
}

func (c *executorClient) send(ctx context.Context, req *ExecutorRequest) (*ExecutorResponse, error) {
	// to implement
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

type ExecutorRequest struct {
	Req interface{}
}

type ExecutorResponse struct {
	resp interface{}
}