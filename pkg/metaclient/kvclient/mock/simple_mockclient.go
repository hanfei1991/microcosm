package mock_metaclient

import (
	"context"
	"strings"
	"sync"

	metaclient "github.com/hanfei1991/microcosm/pkg/metaclient"
)

type mockTxn struct {
	m   *MetaMock
	ops []metaclient.Op
}

func (t *mockTxn) Do(ops ...metaclient.Op) metaclient.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

// [TODO] refine
func (t *mockTxn) Commit() (*metaclient.TxnResponse, metaclient.Error) {
	for _, op := range t.ops {
		_, err := t.m.Put(context.Background(), string(op.KeyBytes()), string(op.ValueBytes()))
		if err != nil {
			return nil, err
		}

	}
	return nil, nil
}

// [TODO] use an embeded etcd as store may be better
// not support Option/txn yet
type MetaMock struct {
	sync.Mutex
	store    map[string]string
	revision int64
}

func NewMetaMock() *MetaMock {
	return &MetaMock{
		store: make(map[string]string),
	}
}

func (m *MetaMock) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, metaclient.Error) {
	m.Lock()
	defer m.Unlock()
	delete(m.store, key)
	m.revision++
	return &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

func (m *MetaMock) Put(ctx context.Context, key, value string) (*metaclient.PutResponse, metaclient.Error) {
	m.Lock()
	defer m.Unlock()
	m.store[key] = value
	m.revision++
	return &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

func (m *MetaMock) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, metaclient.Error) {
	m.Lock()
	defer m.Unlock()
	ret := &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}
	for k, v := range m.store {
		if !strings.HasPrefix(k, key) {
			continue
		}
		ret.Kvs = append(ret.Kvs, &metaclient.KeyValue{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	m.revision++
	return ret, nil
}

func (m *MetaMock) Txn(ctx context.Context) metaclient.Txn {
	return &mockTxn{
		m: m,
	}
}

func (m *MetaMock) Close() error {
	return nil
}
