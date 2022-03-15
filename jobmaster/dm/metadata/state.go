package metadata

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/hanfei1991/microcosm/pkg/metadata"
)

// State represents the state which need to be stored in metadata.
type State interface{}

// Store manages a type of state.
type Store interface {
	CreateState() State
	Key() string
}

// DefaultStore implements some default methods of Store.
type DefaultStore struct {
	Store

	state    State
	kvClient metadata.MetaKV

	mu sync.RWMutex
}

func NewDefaultStore(kvClient metadata.MetaKV) *DefaultStore {
	return &DefaultStore{
		kvClient: kvClient,
	}
}

func (ds *DefaultStore) PutOp(state State) (clientv3.Op, error) {
	v, err := json.Marshal(state)
	return clientv3.OpPut(ds.Key(), string(v)), errors.Trace(err)
}

func (ds *DefaultStore) DeleteOp() clientv3.Op {
	return clientv3.OpDelete(ds.Key())
}

func (ds *DefaultStore) Put(ctx context.Context, state State) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	putOp, err := ds.PutOp(state)
	if err != nil {
		return errors.Trace(err)
	}

	txn := ds.kvClient.Txn(ctx).(clientv3.Txn)
	_, err = txn.Then(putOp).Commit()
	if err != nil {
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
	txn := ds.kvClient.Txn(ctx).(clientv3.Txn)
	_, err := txn.Then(delOp).Commit()
	if err != nil {
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
	rawResp, err := ds.kvClient.Get(ctx, ds.Key())
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := rawResp.(*clientv3.GetResponse)
	if len(result.Kvs) == 0 {
		return nil, errors.New("state not found")
	}

	ds.state = ds.CreateState()
	err = json.Unmarshal(result.Kvs[0].Value, ds.state)
	if err != nil {
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
