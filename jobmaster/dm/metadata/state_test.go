package metadata

import (
	"bytes"
	"context"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/meta/kv/mockclient"
)

type DummyState struct {
	State
	I int
}

func (ds *DummyState) String() string {
	return "dummy state"
}

type DummyStore struct {
	*TomlStore
}

func (ds *DummyStore) CreateState() State {
	return &DummyState{}
}

func (ds *DummyStore) Key() string {
	return "dummy store"
}

type FailedState struct {
	State
	I int
	i int
}

type FailedStore struct {
	*TomlStore
}

func (fs *FailedStore) CreateState() State {
	return &FailedState{}
}

func (fs *FailedStore) Key() string {
	return "failed store"
}

func TestDefaultStore(t *testing.T) {
	t.Parallel()

	kvClient := mockclient.NewMetaMock()
	dummyState := &DummyState{I: 1}
	dummyStore := &DummyStore{
		TomlStore: NewTomlStore(kvClient),
	}
	dummyStore.TomlStore.Store = dummyStore

	state, err := dummyStore.Get(context.Background())
	require.Error(t, err)
	require.Nil(t, state)
	require.NoError(t, dummyStore.Delete(context.Background()))

	require.NoError(t, dummyStore.Put(context.Background(), dummyState))
	state, err = dummyStore.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, dummyState, state)

	dummyState = &DummyState{I: 2}
	require.NoError(t, dummyStore.Put(context.Background(), dummyState))
	state, err = dummyStore.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, dummyState, state)

	require.NoError(t, dummyStore.Delete(context.Background()))
	state, err = dummyStore.Get(context.Background())
	require.Error(t, err)
	require.Nil(t, state)

	var b bytes.Buffer
	err = toml.NewEncoder(&b).Encode(dummyState)
	require.NoError(t, err)
	kvClient.Put(context.Background(), dummyStore.Key(), b.String())
	state, err = dummyStore.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, dummyState, state)
	state, err = dummyStore.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, dummyState, state)

	dummyState = state.(*DummyState)
	require.Equal(t, dummyState.String(), "dummy state")

	failedState := &FailedState{I: 1, i: 2}
	failedStore := &FailedStore{
		TomlStore: NewTomlStore(kvClient),
	}
	failedStore.TomlStore.Store = failedStore
	require.EqualError(t, failedStore.Put(context.Background(), failedState), "fields of state should all be public")
}
