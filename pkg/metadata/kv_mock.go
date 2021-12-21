package metadata

import (
	"context"
	"strings"
	"sync"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

var (
	_ MetaKV       = &MetaMock{}
	_ clientv3.Txn = &Txn{}
)

type Txn struct {
	m   *MetaMock
	ops []clientv3.Op
}

func (t *Txn) If(cs ...clientv3.Cmp) clientv3.Txn {
	panic("unimplemented")
}

func (t *Txn) Else(cs ...clientv3.Op) clientv3.Txn {
	panic("unimplemented")
}

func (t *Txn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

func (t *Txn) Commit() (*clientv3.TxnResponse, error) {
	for _, op := range t.ops {
		_, err := t.m.Put(context.Background(), string(op.KeyBytes()), string(op.ValueBytes()))
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

type MetaMock struct {
	sync.Mutex
	store map[string]string
}

func NewMetaMock() *MetaMock {
	return &MetaMock{
		store: make(map[string]string),
	}
}

func (m *MetaMock) Delete(ctx context.Context, key string, opts ...interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	delete(m.store, key)
	return nil, nil
}

func (m *MetaMock) Watch(ctx context.Context, key string, opts ...interface{}) interface{} {
	panic("unimplemented")
}

func (m *MetaMock) Put(ctx context.Context, key, value string, opts ...interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	m.store[key] = value
	return nil, nil
}

func (m *MetaMock) Get(ctx context.Context, key string, opts ...interface{}) (interface{}, error) {
	m.Lock()
	defer m.Unlock()
	ret := &clientv3.GetResponse{}
	for k, v := range m.store {
		if !strings.HasPrefix(k, key) {
			continue
		}
		ret.Kvs = append(ret.Kvs, &mvccpb.KeyValue{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return ret, nil
}

func (m *MetaMock) Txn(ctx context.Context) interface{} {
	return &Txn{
		m: m,
	}
}
