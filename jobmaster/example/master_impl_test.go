package example

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib"
)

const (
	masterID = "master"

	executorNodeID = "node-exec"

	workerID = "worker"
)

func newExampleMaster() *exampleMaster {
	self := &exampleMaster{}
	self.BaseMaster = lib.MockBaseMaster(masterID, self)
	return self
}

func TestExampleMaster(t *testing.T) {
	t.Parallel()

	_ = log.InitLogger(&log.Config{
		Level: "debug",
	})

	master := newExampleMaster()
	err := lib.MockBaseMasterCreateWorker(
		master.BaseMaster,
		exampleWorkerType,
		exampleWorkerCfg,
		exampleWorkerCost,
		masterID,
		workerID,
		executorNodeID,
	)
	require.NoError(t, err)

	ctx := context.Background()
	err = master.Init(ctx)
	require.NoError(t, err)

	// master.Init will create a worker
	require.Eventually(t, func() bool {
		return master.workerHandle != nil
	}, time.Second, 100*time.Millisecond)
	require.NoError(t, master.receivedErr)

	// Question(lance6717): why there're two ways to get worker handle? will they become inconsistent?
	require.Equal(t, master.workerHandle, master.GetWorkers()[master.workerID])

	err = master.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, master.tickCount)

	// Question(lance6716): what else method can be used to test?

	err = master.Close(ctx)
	require.NoError(t, err)
}
