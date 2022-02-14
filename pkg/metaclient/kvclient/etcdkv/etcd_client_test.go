package etcdkv

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/embed"
)

type SuiteTestEtcd struct {
	// Include basic suite logic.
	suite.Suite
	e         *embed.Etcd
	endpoints string
}

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

// The SetupSuite method will be run by testify once, at the very
// start of the testing suite, before any tests are run.
func (suite *SuiteTestEtcd) SetupSuite() {
	cfg := embed.NewConfig()
	tmpDir := "suite-etcd"
	dir, err := ioutil.TempDir("", tmpDir)
	require.Nil(suite.T(), err)
	cfg.Dir = dir
	peers := allocTempURL(suite.T())
	log.Printf("Allocate server peer port is %s", peers)
	u, err := url.Parse(peers)
	require.Nil(suite.T(), err)
	cfg.LPUrls = []url.URL{*u}
	advertises := allocTempURL(suite.T())
	log.Printf("Allocate server advertises port is %s", advertises)
	u, err = url.Parse(advertises)
	require.Nil(suite.T(), err)
	cfg.LCUrls = []url.URL{*u}
	suite.e, err = embed.StartEtcd(cfg)
	if err != nil {
		require.FailNow(suite.T(), "Start embedded etcd fail:%v", err)
	}
	select {
	case <-suite.e.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		suite.e.Server.Stop() // trigger a shutdown
		suite.e.Close()
		suite.e = nil
		require.FailNow(suite.T(), "Server took too long to start!")
	}
	suite.endpoints = advertises
}

// The TearDownSuite method will be run by testify once, at the very
// end of the testing suite, after all tests have been run.
func (suite *SuiteTestEtcd) TearDownSuite() {
	if suite.e != nil {
		suite.e.Server.Stop()
		suite.e.Close()
	}
}

func clearKeySpace(ctx context.Context, cli metaclient.KVClient) {
	cli.Delete(ctx, "", metaclient.WithFromKey())
}

func (suite *SuiteTestEtcd) TestBasicKV() {
	conf := &metaclient.Config{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdImpl(conf)
	require.Nil(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// get a non-existed key
	grsp, gerr := cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	cluster := grsp.Header.ClusterID
	require.NotEmpty(t, cluster)
	require.Len(t, grsp.Kvs, 0)

	// put a key and get it back
	prsp, perr := cli.Put(ctx, "hello", "world")
	require.Nil(t, perr)
	require.Equal(t, cluster, prsp.Header.ClusterID)
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
	require.Equal(t, "world", string(grsp.Kvs[0].Value))

	// delete a key, get it back, put the same key and get it back
	drsp, derr := cli.Delete(ctx, "hello")
	require.Nil(t, derr)
	require.Equal(t, cluster, drsp.Header.ClusterID)
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 0)
	prsp, perr = cli.Put(ctx, "hello", "world again")
	require.Nil(t, perr)
	require.Equal(t, cluster, prsp.Header.ClusterID)
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
	require.Equal(t, "world again", string(grsp.Kvs[0].Value))
	clearKeySpace(ctx, cli)
	cli.Close()
	// [TODO] using failpoint to test basic kv timeout and cancel
}

func prepareDataForRangeTest(ctx context.Context, t *testing.T, cli metaclient.KVClient) {
	prsp, perr := cli.Put(ctx, "hello1", "world1")
	require.Nil(t, perr)
	cluster := prsp.Header.ClusterID
	require.NotEmpty(t, cluster)
	prsp, perr = cli.Put(ctx, "hello2", "world2")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "interesting", "world")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "dataflow", "engine")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "TiDB", "component")
	require.Nil(t, perr)
}

func (suite *SuiteTestEtcd) TestKeyRangeOption() {
	conf := &metaclient.Config{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdImpl(conf)
	require.Nil(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// test get key range(WithRange/WithPrefix/WithFromKey)
	prepareDataForRangeTest(ctx, t, cli)
	defer clearKeySpace(ctx, cli)

	grsp, gerr := cli.Get(ctx, "hello", metaclient.WithRange("s"))
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 3) // hello1, hello2, interesting
	require.Equal(t, string(grsp.Kvs[0].Key), "hello1")
	require.Equal(t, string(grsp.Kvs[0].Value), "world1")
	require.Equal(t, string(grsp.Kvs[1].Key), "hello2")
	require.Equal(t, string(grsp.Kvs[1].Value), "world2")
	require.Equal(t, string(grsp.Kvs[2].Key), "interesting")
	require.Equal(t, string(grsp.Kvs[2].Value), "world")

	grsp, gerr = cli.Get(ctx, "hello2", metaclient.WithRange("Z"))
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 0)

	grsp, gerr = cli.Get(ctx, "hello", metaclient.WithPrefix())
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 2)
	require.Equal(t, string(grsp.Kvs[0].Key), "hello1")
	require.Equal(t, string(grsp.Kvs[0].Value), "world1")
	require.Equal(t, string(grsp.Kvs[1].Key), "hello2")
	require.Equal(t, string(grsp.Kvs[1].Value), "world2")

	grsp, gerr = cli.Get(ctx, "Hello", metaclient.WithFromKey())
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 5) // TiDB, dataflow, hello1, hello2, interesting
	require.Equal(t, string(grsp.Kvs[0].Key), "TiDB")
	require.Equal(t, string(grsp.Kvs[0].Value), "component")
	require.Equal(t, string(grsp.Kvs[1].Key), "dataflow")
	require.Equal(t, string(grsp.Kvs[1].Value), "engine")
	require.Equal(t, string(grsp.Kvs[2].Key), "hello1")
	require.Equal(t, string(grsp.Kvs[2].Value), "world1")
	require.Equal(t, string(grsp.Kvs[3].Key), "hello2")
	require.Equal(t, string(grsp.Kvs[3].Value), "world2")
	require.Equal(t, string(grsp.Kvs[4].Key), "interesting")
	require.Equal(t, string(grsp.Kvs[4].Value), "world")

	_, derr := cli.Delete(ctx, "hello", metaclient.WithPrefix())
	require.Nil(t, derr)
	grsp, gerr = cli.Get(ctx, "", metaclient.WithFromKey())
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 3) // TiDB, dataflow, hello1, hello2, interesting
	require.Equal(t, string(grsp.Kvs[0].Key), "TiDB")
	require.Equal(t, string(grsp.Kvs[0].Value), "component")
	require.Equal(t, string(grsp.Kvs[1].Key), "dataflow")
	require.Equal(t, string(grsp.Kvs[1].Value), "engine")
	require.Equal(t, string(grsp.Kvs[2].Key), "interesting")
	require.Equal(t, string(grsp.Kvs[2].Value), "world")

	_, derr = cli.Delete(ctx, "AZ", metaclient.WithRange("Titan"))
	require.Nil(t, derr)
	grsp, gerr = cli.Get(ctx, "", metaclient.WithFromKey())
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 2) // TiDB, dataflow, hello1, hello2, interesting
	require.Equal(t, string(grsp.Kvs[0].Key), "dataflow")
	require.Equal(t, string(grsp.Kvs[0].Value), "engine")
	require.Equal(t, string(grsp.Kvs[1].Key), "interesting")
	require.Equal(t, string(grsp.Kvs[1].Value), "world")

	_, derr = cli.Delete(ctx, "egg", metaclient.WithFromKey())
	require.Nil(t, derr)
	grsp, gerr = cli.Get(ctx, "", metaclient.WithFromKey())
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
}

func prepareDataForTxnTest(ctx context.Context, t *testing.T, cli metaclient.KVClient) {
	prsp, perr := cli.Put(ctx, "hello1", "world1")
	require.Nil(t, perr)
	cluster := prsp.Header.ClusterID
	require.NotEmpty(t, cluster)
	prsp, perr = cli.Put(ctx, "hello2", "world2")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "interesting", "world")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "dataflow", "engine")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "TiDB", "component")
	require.Nil(t, perr)
}

func (suite *SuiteTestEtcd) TestTxn() {
	conf := &metaclient.Config{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdImpl(conf)
	require.Nil(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	prepareDataForTxnTest(ctx, t, cli)
	defer clearKeySpace(ctx, cli)

	// etcd forbits same key op intersect(put/delete) in txn to avoid quadratic blowup??
	txn := cli.Txn(ctx)
	txn = txn.Do(metaclient.OpGet("hello"), metaclient.OpGet("hello", metaclient.WithPrefix()))
	txn.Do(metaclient.OpPut("hello", "world"))
	txn.Do(metaclient.OpDelete("hello"))
	txn.Do(metaclient.OpGet("", metaclient.WithFromKey()))
	rsp, err := txn.Commit()
	require.Error(t, err)
	require.Nil(t, rsp)

	txn = cli.Txn(ctx)
	txn = txn.Do(metaclient.OpGet("hello"), metaclient.OpGet("hell", metaclient.WithPrefix()))
	txn.Do(metaclient.OpPut("hello3", "world3"), metaclient.OpPut("dataflow2", "engine2"))
	txn = txn.Do(metaclient.OpDelete("dataflow3")).Do(metaclient.OpDelete("inte", metaclient.WithPrefix()))
	txn.Do(metaclient.OpGet("", metaclient.WithFromKey()))
	rsp, err = txn.Commit()
	require.Nil(t, err)
	require.NotNil(t, rsp)
	require.Len(t, rsp.Responses, 7)

	getRsp := rsp.Responses[0].GetResponseGet()
	require.NotNil(t, getRsp)
	require.Len(t, getRsp.Kvs, 0)
	getRsp = rsp.Responses[1].GetResponseGet()
	require.NotNil(t, getRsp)
	require.Len(t, getRsp.Kvs, 2)
	require.Equal(t, "hello1", string(getRsp.Kvs[0].Key))
	require.Equal(t, "world1", string(getRsp.Kvs[0].Value))
	require.Equal(t, "hello2", string(getRsp.Kvs[1].Key))
	require.Equal(t, "world2", string(getRsp.Kvs[1].Value))

	putRsp := rsp.Responses[2].GetResponsePut()
	require.NotNil(t, putRsp)
	putRsp = rsp.Responses[3].GetResponsePut()
	require.NotNil(t, putRsp)

	delRsp := rsp.Responses[4].GetResponseDelete()
	require.NotNil(t, delRsp)
	delRsp = rsp.Responses[5].GetResponseDelete()
	require.NotNil(t, delRsp)

	grsp := rsp.Responses[6].GetResponseGet()
	require.NotNil(t, grsp)
	require.Len(t, grsp.Kvs, 6)
	require.Equal(t, string(grsp.Kvs[0].Key), "TiDB")
	require.Equal(t, string(grsp.Kvs[0].Value), "component")
	require.Equal(t, string(grsp.Kvs[1].Key), "dataflow")
	require.Equal(t, string(grsp.Kvs[1].Value), "engine")
	require.Equal(t, string(grsp.Kvs[2].Key), "dataflow2")
	require.Equal(t, string(grsp.Kvs[2].Value), "engine2")
	require.Equal(t, string(grsp.Kvs[3].Key), "hello1")
	require.Equal(t, string(grsp.Kvs[3].Value), "world1")
	require.Equal(t, string(grsp.Kvs[4].Key), "hello2")
	require.Equal(t, string(grsp.Kvs[4].Value), "world2")
	require.Equal(t, string(grsp.Kvs[5].Key), "hello3")
	require.Equal(t, string(grsp.Kvs[5].Value), "world3")
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestEtcdSuite(t *testing.T) {
	suite.Run(t, new(SuiteTestEtcd))
}
