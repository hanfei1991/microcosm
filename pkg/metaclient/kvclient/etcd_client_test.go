package kvclient

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

func (suite *SuiteTestEtcd) TestBasicKV() {
	conf := &metaclient.Config{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdKVClient(conf, "TestBasicKV")
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
	revision := grsp.Kvs[0].Revision
	log.Printf("revision is %d", revision)
	require.Greater(t, revision, int64(0))
	require.Equal(t, "world", string(grsp.Kvs[0].Value))
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
	require.Equal(t, revision, grsp.Kvs[0].Revision)
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
	// after a delete + put, we still expect an increasing revision to avoid ABA problem
	require.Greater(t, grsp.Kvs[0].Revision, revision)
	revision = grsp.Kvs[0].Revision
	log.Printf("revision is %d", revision)
	require.Equal(t, "world again", string(grsp.Kvs[0].Value))
	cli.Close()
	// [TODO] using failpoint to test basic kv timeout and cancel
}

func (suite *SuiteTestEtcd) TestIdempotentOption() {
	conf := &metaclient.Config{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdKVClient(conf, "TestOptions")
	require.Nil(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// test idempotent put
	prsp, perr := cli.Put(ctx, "hello", "world")
	require.Nil(t, perr)
	cluster := prsp.Header.ClusterID
	require.NotEmpty(t, cluster)
	grsp, gerr := cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
	revision := grsp.Kvs[0].Revision
	log.Printf("revision is %d", revision)
	require.Greater(t, revision, int64(0))
	require.Equal(t, "world", string(grsp.Kvs[0].Value))
	// right revision
	prsp, perr = cli.Put(ctx, "hello", "world2", metaclient.WithRevision(revision))
	require.Nil(t, perr)
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
	require.Greater(t, grsp.Kvs[0].Revision, revision)
	revision2 := grsp.Kvs[0].Revision
	require.Equal(t, "world2", string(grsp.Kvs[0].Value))
	// wrong revision
	prsp, perr = cli.Put(ctx, "hello", "world2", metaclient.WithRevision(revision))
	require.NotNil(t, perr)
	//require.ErrorAs(t, perr, cerrors.ErrMetaRevisionUnmatch)
	// [TODO] check the specified error
	require.Nil(t, prsp)
	// right revision
	prsp, perr = cli.Put(ctx, "hello", "world3", metaclient.WithRevision(revision2))
	require.Nil(t, perr)
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
	require.Greater(t, grsp.Kvs[0].Revision, revision2)
	revision2 = grsp.Kvs[0].Revision
	require.Equal(t, "world3", string(grsp.Kvs[0].Value))

	// test idempotent delete
	// wrong version
	drsp, derr := cli.Delete(ctx, "hello", metaclient.WithRevision(revision))
	require.NotNil(t, derr)
	require.Nil(t, drsp)
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 1)
	require.Equal(t, revision2, grsp.Kvs[0].Revision)
	revision2 = grsp.Kvs[0].Revision
	require.Equal(t, "world3", string(grsp.Kvs[0].Value))

	// right version
	drsp, derr = cli.Delete(ctx, "hello", metaclient.WithRevision(revision2))
	require.Nil(t, derr)
	grsp, gerr = cli.Get(ctx, "hello")
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 0)
	cli.Close()
}

func (suite *SuiteTestEtcd) TestTTLOption() {
	// [TODO] ttl not support quite well now
}

func (suite *SuiteTestEtcd) TestKeyRangeOption() {
	conf := &metaclient.Config{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdKVClient(conf, "TestKeyRangeOption")
	require.Nil(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// test get key range
	prsp, perr := cli.Put(ctx, "hello", "world")
	require.Nil(t, perr)
	cluster := prsp.Header.ClusterID
	require.NotEmpty(t, cluster)
	prsp, perr = cli.Put(ctx, "hello1", "world1")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "hello2", "world2")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "interesting", "world")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "dataflow", "engine")
	require.Nil(t, perr)
	prsp, perr = cli.Put(ctx, "TiDB", "component")
	require.Nil(t, perr)

	grsp, gerr := cli.Get(ctx, "hello", metaclient.WithRange("z"))
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 4)
	grsp, gerr = cli.Get(ctx, "hello2", metaclient.WithRange("Z"))
	require.Nil(t, gerr)
	require.Len(t, grsp.Kvs, 0)
}

func (suite *SuiteTestEtcd) TestTxn() {

}

func (suite *SuiteTestEtcd) TestConfig() {
}

func (suite *SuiteTestEtcd) TestNamspace() {
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestEtcdSuite(t *testing.T) {
	suite.Run(t, new(SuiteTestEtcd))
}
