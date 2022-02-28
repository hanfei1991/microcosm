package e2e_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	DemoAddress             = "127.0.0.1:1234"
	MasterAddressList       = []string{"127.0.0.1:10245", "127.0.0.1:10246", "127.0.0.1:10247"}
	RecordNum         int64 = 10000
	DemoConfig              = `
{
   "srcHost":"demo-server:1234",
   "srcDir":"/data",
   "dstHost":"demo-server:1234",
   "dstDir":"/data1"
}
`
)

type DemoClient struct {
	conn   *grpc.ClientConn
	client pb.DataRWServiceClient
}

func NewDemoClient(ctx context.Context, addr string) (*DemoClient, error) {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return &DemoClient{
		conn:   conn,
		client: pb.NewDataRWServiceClient(conn),
	}, err
}

// run this test after docker-compose has been up
func TestSubmitTest(t *testing.T) {
	ctx := context.Background()
	democlient, err := NewDemoClient(ctx, DemoAddress)
	require.Nil(t, err)
	masterclient, err := client.NewMasterClient(ctx, MasterAddressList)
	require.Nil(t, err)

	for {
		resp, err := democlient.client.IsReady(ctx, &pb.IsReadyRequest{})
		require.Nil(t, err)
		if resp.Ready {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	resp, err := masterclient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: []byte(DemoConfig),
	})
	require.Nil(t, err)
	require.Nil(t, resp.Err)

	queryReq := &pb.QueryJobRequest{
		JobId: resp.JobIdStr,
	}
	// continue to query
	for {
		queryResp, err := masterclient.QueryJob(ctx, queryReq)
		require.Nil(t, err)
		time.Sleep(10 * time.Millisecond)
		require.Nil(t, err)
		require.Nil(t, queryResp.Err)
		require.Equal(t, queryResp.Tp, int64(lib.CvsJobMaster))
		if queryResp.Status == pb.QueryJobResponse_online {
			statusBytes := queryResp.JobMasterInfo.Status
			status := &lib.WorkerStatus{}
			err = json.Unmarshal(statusBytes, status)
			require.Nil(t, err)
			ext, err := strconv.ParseInt(string(status.ExtBytes), 10, 64)
			require.Nil(t, err)
			log.L().Info("worker online", zap.Int64("ext", ext))
			if ext == RecordNum {
				break
			}
		} else {
			require.Equal(t, queryResp.Status, pb.QueryJobResponse_dispatched)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// check files
	demoResp, err := democlient.client.CheckDir(ctx, &pb.CheckDirRequest{
		Dir: "/data1",
	})
	require.Nil(t, err)
	require.Empty(t, demoResp.ErrFileName, demoResp.ErrMsg)
}
