package executor

import (
	"errors"
	"strings"

	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/context"
	"google.golang.org/grpc"
)

type MasterClient struct {
	cfg *Config

	urls []string
	leader string
	conn *grpc.ClientConn
	client pb.MasterClient
}

func getJoinURLs(addrs string) []string {
	return strings.Split(addrs,",")
}

func NewMasterClient(cfg *Config) (*MasterClient, error) {
	client := &MasterClient{
		cfg: cfg,
	}
	client.urls = getJoinURLs(cfg.Join)
	client.leader = client.urls[0]
	var err error
	client.conn, err = grpc.Dial(client.leader, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.New("cannot build conn")
	}
	client.client = pb.NewMasterClient(client.conn)
	return client, nil
}

func (c *MasterClient) SendHeartbeat(req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return c.client.Heartbeat(context.Background().Ctx, req)
}

func (c *MasterClient) RegisterExecutor(req *pb.RegisterExecutorRequest) (resp *pb.RegisterExecutorResponse, err error) {
	return c.client.RegisterExecutor(context.Background().Ctx, req)
}