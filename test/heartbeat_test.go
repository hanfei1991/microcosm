package test_test

import (
	"testing"

	"github.com/hanfei1991/microcosom/executor"
	"github.com/hanfei1991/microcosom/master"
	"github.com/hanfei1991/microcosom/test"
	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	test.GlobalTestFlag = true
	TestingT(t)
}

type testHeartbeatSuite struct{
	master *master.Server
	executor *executor.Server
}

func (t *testHeartbeatSuite) SetUpSuite(c *C) {
	masterCfg := &master.Config{
		Name : "master1",
		MasterAddr: "127.0.0.1:1991",
		DataDir: "/tmp/df",
		KeepAliveTTLStr: "3s",
		KeepAliveIntervalStr: "500ms",
		RPCTimeoutStr: "6s",
	}
	var err error
	t.master, err = master.NewServer(masterCfg)
	c.Assert(err, IsNil)
	// one master + one executor
	executorCfg := &executor.Config{
		Join: "127.0.0.1:1991",
		WorkerAddr: "127.0.0.1:1992",
	}
	t.executor = executor.NewServer()
}
