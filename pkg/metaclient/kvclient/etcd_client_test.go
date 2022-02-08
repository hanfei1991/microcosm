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
	u, err := url.Parse(peers)
	require.Nil(suite.T(), err)
	cfg.LPUrls = []url.URL{*u}
	advertises := allocTempURL(suite.T())
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
	cli, err := NewEtcdKVClient(conf, "111")
	require.Nil(suite.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	rsp, err := cli.Get(ctx, "key1")
	require.Nil(suite.T(), err)
	require.Len(suite.T(), rsp.Kvs, 0)
	cancel()
}

func (suite *SuiteTestEtcd) TestOptions() {
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
