package servermaster

import (
	"testing"

	metacom "github.com/hanfei1991/microcosm/pkg/meta/common"
	"github.com/stretchr/testify/require"
)

func TestMetaStoreManager(t *testing.T) {
	t.Parallel()

	storeConf := &metacom.StoreConfigParams{
		Endpoints: []string{
			"127.0.0.1",
			"127.0.0.2",
		},
	}

	manager := NewMetaStoreManager()
	err := manager.Register("root", storeConf)
	require.Nil(t, err)

	err = manager.Register("root", &metacom.StoreConfigParams{})
	require.Error(t, err)

	store := manager.GetMetaStore("default")
	require.Nil(t, store)

	store = manager.GetMetaStore("root")
	require.NotNil(t, store)
	require.Equal(t, storeConf, store)

	manager.UnRegister("root")
	store = manager.GetMetaStore("root")
	require.Nil(t, store)
}

func TestDefaultMetaStoreManager(t *testing.T) {
	t.Parallel()

	store := NewFrameMetaConfig()
	require.Equal(t, metacom.FrameMetaID, store.StoreID)
	require.Equal(t, metacom.DefaultFrameMetaEndpoints, store.Endpoints[0])

	store = NewDefaultUserMetaConfig()
	require.Equal(t, metacom.DefaultUserMetaID, store.StoreID)
	require.Equal(t, metacom.DefaultUserMetaEndpoints, store.Endpoints[0])
}
