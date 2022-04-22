package kv

import (
	"context"
	"fmt"
	"strings"
	"sync"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
)

type mockTxn struct {
	c   context.Context
	m   *MetaMock
	ops []kvclient.Op
}

func (t *mockTxn) Do(ops ...kvclient.Op) kvclient.Txn {
	t.ops = append(t.ops, ops...)
	return t
}

func (t *mockTxn) Commit() (*kvclient.TxnResponse, kvclient.Error) {
	txnRsp := &kvclient.TxnResponse{
		Header: &kvclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
		Responses: make([]kvclient.ResponseOp, 0, len(t.ops)),
	}

	// we lock the MetaMock to simulate the SERIALIZABLE isolation
	t.m.Lock()
	defer t.m.Unlock()

	for _, op := range t.ops {
		rsp, err := t.m.DoNoLock(t.c, op)
		if err != nil {
			return nil, err
		}
		switch {
		case op.IsGet():
			txnRsp.Responses = append(txnRsp.Responses, kvclient.ResponseOp{
				Response: &kvclient.ResponseOpResponseGet{
					ResponseGet: rsp.Get(),
				},
			})
		case op.IsPut():
			txnRsp.Responses = append(txnRsp.Responses, kvclient.ResponseOp{
				Response: &kvclient.ResponseOpResponsePut{
					ResponsePut: rsp.Put(),
				},
			})
		case op.IsDelete():
			txnRsp.Responses = append(txnRsp.Responses, kvclient.ResponseOp{
				Response: &kvclient.ResponseOpResponseDelete{
					ResponseDelete: rsp.Del(),
				},
			})
		default:
			return nil, &mockError{
				caused: cerrors.ErrMetaOptionInvalid.Wrap(fmt.Errorf("unrecognized op type:%d", op.T)),
			}
		}
	}

	return txnRsp, nil
}

// not support Option yet
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

func (m *MetaMock) Delete(ctx context.Context, key string, opts ...kvclient.OpOption) (*kvclient.DeleteResponse, kvclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.DeleteNoLock(ctx, key, opts...)
}

func (m *MetaMock) DeleteNoLock(ctx context.Context, key string, opts ...kvclient.OpOption) (*kvclient.DeleteResponse, kvclient.Error) {
	delete(m.store, key)
	m.revision++
	return &kvclient.DeleteResponse{
		Header: &kvclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

func (m *MetaMock) Put(ctx context.Context, key, value string) (*kvclient.PutResponse, kvclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.PutNoLock(ctx, key, value)
}

func (m *MetaMock) PutNoLock(ctx context.Context, key, value string) (*kvclient.PutResponse, kvclient.Error) {
	m.store[key] = value
	m.revision++
	return &kvclient.PutResponse{
		Header: &kvclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}, nil
}

func (m *MetaMock) Get(ctx context.Context, key string, opts ...kvclient.OpOption) (*kvclient.GetResponse, kvclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.GetNoLock(ctx, key, opts...)
}

func (m *MetaMock) GetNoLock(ctx context.Context, key string, opts ...kvclient.OpOption) (*kvclient.GetResponse, kvclient.Error) {
	ret := &kvclient.GetResponse{
		Header: &kvclient.ResponseHeader{
			ClusterID: "mock_cluster",
		},
	}
	for k, v := range m.store {
		if !strings.HasPrefix(k, key) {
			continue
		}
		ret.Kvs = append(ret.Kvs, &kvclient.KeyValue{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}
	return ret, nil
}

func (m *MetaMock) Do(ctx context.Context, op kvclient.Op) (kvclient.OpResponse, kvclient.Error) {
	m.Lock()
	defer m.Unlock()

	return m.DoNoLock(ctx, op)
}

func (m *MetaMock) DoNoLock(ctx context.Context, op kvclient.Op) (kvclient.OpResponse, kvclient.Error) {
	switch {
	case op.IsGet():
		rsp, err := m.GetNoLock(ctx, string(op.KeyBytes()))
		if err != nil {
			return kvclient.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	case op.IsDelete():
		rsp, err := m.DeleteNoLock(ctx, string(op.KeyBytes()))
		if err != nil {
			return kvclient.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	case op.IsPut():
		rsp, err := m.PutNoLock(ctx, string(op.KeyBytes()), string(op.ValueBytes()))
		if err != nil {
			return kvclient.OpResponse{}, err
		}
		return rsp.OpResponse(), nil
	default:
	}

	return kvclient.OpResponse{}, &mockError{
		caused: cerrors.ErrMetaOptionInvalid.Wrap(fmt.Errorf("unrecognized op type:%d", op.T)),
	}
}

func (m *MetaMock) Txn(ctx context.Context) kvclient.Txn {
	return &mockTxn{
		m: m,
		c: ctx,
	}
}

func (m *MetaMock) Close() error {
	return nil
}

func (m *MetaMock) GenEpoch(ctx context.Context) (int64, error) {
	m.Lock()
	defer m.Unlock()

	m.revision++
	return m.revision, nil
}

type mockError struct {
	caused error
}

func (e *mockError) IsRetryable() bool {
	return false
}

func (e *mockError) Error() string {
	return e.caused.Error()
}
