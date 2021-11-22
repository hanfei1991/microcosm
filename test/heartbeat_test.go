package test_test

import (
	"context"
	"testing"
	"time"

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

	masterCtx *test.Context
	executorCtx *test.Context
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
	t.masterCtx = test.NewContext()
	t.master, err = master.NewServer(masterCfg, t.masterCtx)
	c.Assert(err, IsNil)
	// one master + one executor
	executorCfg := &executor.Config{
		Join: "127.0.0.1:1991",
		WorkerAddr: "127.0.0.1:1992",
	}
	t.executorCtx = test.NewContext()
	t.executor = executor.NewServer(executorCfg, t.executorCtx)
}

func (t *testHeartbeatSuite) TestHeartbeatExecutorCrush(c *C) {
	ctx := context.Background()
	masterCtx, _ := context.WithCancel(ctx)
	t.master.Start(masterCtx)
	execCtx, execCancel := context.WithCancel(ctx)
	t.executor.Start(execCtx)

	time.Sleep(2 * time.Second)
	execCancel()

	executorEvent := <- t.executorCtx.ExcutorChange()
	masterEvent := <- t.masterCtx.ExcutorChange()
	c.Assert(executorEvent.Time.Add(3 * time.Second), Less, masterEvent.Time)
}
