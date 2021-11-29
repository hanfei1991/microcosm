package system

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// TODO: add interface abstraction
// MasterClient is owned by job master, and requests to server master
type MasterClient struct {
	urls   []string
	leader string
	conn   *grpc.ClientConn
	client pb.MasterClient
}

func (c *MasterClient) init(ctx context.Context) error {
	log.L().Logger.Info("dialing master", zap.String("leader", c.leader))
	conn, err := grpc.DialContext(ctx, c.leader, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return errors.ErrGrpcBuildConn.GenWithStackByArgs(c.leader)
	}
	c.client = pb.NewMasterClient(conn)
	c.conn = conn
	return nil
}

// NewMaserClient creates a new server master rpc client that is used by job master
func NewMasterClient(ctx context.Context, join []string) (*MasterClient, error) {
	client := &MasterClient{
		urls: join,
	}
	client.leader = client.urls[0]
	err := client.init(ctx)
	return client, err
}

// Send TaskSchedulerRequest and allocates executors to given tasks
func (c *MasterClient) RequestForSchedule(
	ctx context.Context,
	req *pb.TaskSchedulerRequest,
	timeout time.Duration,
) (*pb.TaskSchedulerResponse, error) {
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	return c.client.ScheduleTask(ctx1, req)
}
