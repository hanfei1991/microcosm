package metadata

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

// State represents the state which need to be stored in metadata.
type State interface{}

// Store manages a type of state.
// Store provides the factory, some utility functions and persistence of State.
type Store interface {
	CreateState() State
	Key() string
}

// DefaultStore implements some default methods of Store.
type DefaultStore struct {
	Store

	state    State
	kvClient metaclient.KVClient

	mu sync.RWMutex
}

func NewDefaultStore(kvClient metaclient.KVClient) *DefaultStore {
	return &DefaultStore{
		kvClient: kvClient,
	}
}

func (ds *DefaultStore) PutOp(state State) (metaclient.Op, error) {
	v, err := json.Marshal(state)
	return metaclient.OpPut(ds.Key(), string(v)), errors.Trace(err)
}

func (ds *DefaultStore) DeleteOp() metaclient.Op {
	return metaclient.OpDelete(ds.Key())
}

func (ds *DefaultStore) Put(ctx context.Context, state State) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	putOp, err := ds.PutOp(state)
	if err != nil {
		return errors.Trace(err)
	}

	if _, err = ds.kvClient.Txn(ctx).Do(putOp).Commit(); err != nil {
		return errors.Trace(err)
	}

	ds.state = state
	return nil
}

func (ds *DefaultStore) Delete(ctx context.Context) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.state == nil {
		return nil
	}

	delOp := ds.DeleteOp()
	if _, err := ds.kvClient.Txn(ctx).Do(delOp).Commit(); err != nil {
		return errors.Trace(err)
	}

	ds.state = nil
	return nil
}

// Notice: get always return clone of a state.
func (ds *DefaultStore) Get(ctx context.Context) (State, error) {
	ds.mu.RLock()
	if ds.state != nil {
		clone, err := ds.cloneState()
		ds.mu.RUnlock()
		return clone, err
	}
	ds.mu.RUnlock()

	ds.mu.Lock()
	defer ds.mu.Unlock()

	// check again with write lock
	if ds.state != nil {
		clone, err := ds.cloneState()
		ds.mu.RUnlock()
		return clone, err
	}

	resp, err := ds.kvClient.Get(ctx, ds.Key())
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("state not found")
	}

	ds.state = ds.CreateState()
	if err := json.Unmarshal(resp.Kvs[0].Value, ds.state); err != nil {
		return nil, errors.Trace(err)
	}

	return ds.cloneState()
}

func (ds *DefaultStore) cloneState() (State, error) {
	if ds.state == nil {
		return nil, nil
	}

	clone := ds.CreateState()
	v, err := json.Marshal(ds.state)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = json.Unmarshal(v, clone)
	return clone, errors.Trace(err)
}
