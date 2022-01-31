package kvclient

import (
	"log"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
)

type SuiteTestEtcd struct {
	// Include basic suite logic.
	Suite
	e *embed.Etcd
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
		require.Nil(t, err)
		cfg.Dir = dir 
		peers := allocTempURL(t)
		cfg.LPUrls, _ := url.Parse(peers)
		advertises := allocTempURL(t)
		cfg.LCUrls, _ := url.Parse(advertises)
		suite.e, err := embed.StartEtcd(cfg)
		if err != nil {
			require.FailNow(t, "Start embedded etcd fail:%v", err)
		}
		select {
		case <-suite.e.Server.ReadyNotify():
			log.Printf("Server is ready!")
		case <-time.After(60 * time.Second):
			suite.e.Server.Stop() // trigger a shutdown
			suite.e.Close()
			suite.e = nil
			require.FailNow(t, "Server took too long to start!")
		}
		suite.endpoints = cfg.LCUrls
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
	revision := 0
	cli, err := metaclient.NewEtcdKVClient(conf, "111")	
	require.Nil(t, err)
	ctx, cancel := Context.WithTimeout(Context.Background, 3*time.Second)
	rsp, err := cli.Get(ctx, "key1")
	requie.Nil(t, err)
	require.Greater(t, rsp.Header.Revision, revision)
	require.Len(t, rsp.Kvs, 0)
	revision = rsp.Header.Revision


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
