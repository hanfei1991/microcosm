package dm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

func mockMaster(t *testing.T, id lib.MasterID) *SubTaskMaster {
	ret := &SubTaskMaster{
		id: id,
	}
	ret.BaseMaster = lib.NewBaseMaster(
		ret,
		id,
		p2p.NewMockMessageHandlerManager(),
		p2p.NewMockMessageSender(),
		metadata.NewMetaMock(),
		client.NewClientManager(),
		&client.MockServerMasterClient{},
	)
	cfg := &config.SubTaskConfig{
		From: config.DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "123456",
		},
		To: config.DBConfig{
			Host:     "127.0.0.1",
			Port:     4000,
			User:     "root",
			Password: "",
		},
		ServerID:   102,
		MetaSchema: "db_test",
		Name:       "db_ut",
		Mode:       config.ModeAll,
		Flavor:     "mysql",
	}
	cfg.From.Adjust()
	cfg.To.Adjust()
	ret.cfg = cfg

	masterKey := adapter.MasterInfoKey.Encode(string(id))
	masterInfo := &lib.MasterMetaKVData{
		ID:     id,
		NodeID: "test-node-id",
	}
	masterInfoBytes, err := json.Marshal(masterInfo)
	require.NoError(t, err)
	_, err = ret.MetaKVClient().Put(context.Background(), masterKey, string(masterInfoBytes))
	require.NoError(t, err)

	return ret
}

func TestSubtaskMaster(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := mockMaster(t, "test-subtask-master")
	err := master.Init(ctx)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	err = master.Close(ctx)
	require.NoError(t, err)
}
