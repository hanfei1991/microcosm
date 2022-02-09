// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package namespace

import (
	"context"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
)

type kvPrefixClient struct {
	metaclient.KVClient
	pfx string
}

// NewPrefixKV wraps a KVClient instance so that all requests
// are prefixed with a given string.
func NewPrefixKVClient(kvc metaclient.KVClient, prefix string) metaclient.KVClient {
	return &kvPrefixClient{kvc, prefix}
}

func (kvc *kvPrefixClient) Put(ctx context.Context, key, val string, opts ...metaclient.OpOption) (*metaclient.PutResponse, error) {
	if len(key) == 0 {
		return nil, cerrors.ErrMetaEmptyKey
	}
	orgOp, err := metaclient.OpPut(key, val, opts...)
	if err != nil {
		return nil, err
	}
	op := kvc.prefixOp(orgOp)
	r, err := kvc.KVClient.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	put := r.Put()
	kvc.unprefixPutResponse(put)
	return put, nil
}

func (kvc *kvPrefixClient) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, error) {
	if len(key) == 0 && !(metaclient.IsOptsWithFromKey(opts) || metaclient.IsOptsWithPrefix(opts)) {
		return nil, cerrors.ErrMetaEmptyKey
	}
	orgOp, err := metaclient.OpGet(key, opts...)
	if err != nil {
		return nil, err
	}
	r, err := kvc.KVClient.Do(ctx, kvc.prefixOp(orgOp))
	if err != nil {
		return nil, err
	}
	get := r.Get()
	kvc.unprefixGetResponse(get)
	return get, nil
}

func (kvc *kvPrefixClient) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, error) {
	if len(key) == 0 && !(metaclient.IsOptsWithFromKey(opts) || metaclient.IsOptsWithPrefix(opts)) {
		return nil, cerrors.ErrMetaEmptyKey
	}
	orgOp, err := metaclient.OpDelete(key, opts...)
	if err != nil {
		return nil, err
	}
	r, err := kvc.KVClient.Do(ctx, kvc.prefixOp(orgOp))
	if err != nil {
		return nil, err
	}
	del := r.Del()
	kvc.unprefixDeleteResponse(del)
	return del, nil
}

func (kvc *kvPrefixClient) Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, error) {
	if len(op.KeyBytes()) == 0 && !op.IsTxn() {
		return metaclient.OpResponse{}, cerrors.ErrMetaEmptyKey
	}
	r, err := kvc.KVClient.Do(ctx, kvc.prefixOp(op))
	if err != nil {
		return r, err
	}
	switch {
	case r.Get() != nil:
		kvc.unprefixGetResponse(r.Get())
	case r.Put() != nil:
		kvc.unprefixPutResponse(r.Put())
	case r.Del() != nil:
		kvc.unprefixDeleteResponse(r.Del())
	case r.Txn() != nil:
		kvc.unprefixTxnResponse(r.Txn())
	}
	return r, nil
}

type txnPrefix struct {
	metaclient.Txn
	kvc *kvPrefixClient
}

func (kvc *kvPrefixClient) Txn(ctx context.Context) metaclient.Txn {
	return &txnPrefix{kvc.KVClient.Txn(ctx), kvc}
}

func (kvc *kvPrefixClient) Close() {
	kvc.KVClient.Close()
}

func (txn *txnPrefix) Do(ops ...metaclient.Op) metaclient.Txn {
	txn.Txn = txn.Txn.Do(txn.kvc.prefixOps(ops)...)
	return txn
}

func (txn *txnPrefix) Commit() (*metaclient.TxnResponse, error) {
	resp, err := txn.Txn.Commit()
	if err != nil {
		return nil, err
	}
	txn.kvc.unprefixTxnResponse(resp)
	return resp, nil
}

func (kvc *kvPrefixClient) prefixOp(op metaclient.Op) metaclient.Op {
	if !op.IsTxn() {
		begin, end := kvc.prefixInterval(op.KeyBytes(), op.RangeBytes())
		op.WithKeyBytes(begin)
		op.WithRangeBytes(end)
		return op
	}
	op, err := metaclient.OpTxn(kvc.prefixOps(op.Txn()))
	if err != nil {
		panic("unreachable b")
	}
	return op
}

func (kvc *kvPrefixClient) unprefixGetResponse(resp *metaclient.GetResponse) {
	for i := range resp.Kvs {
		resp.Kvs[i].Key = resp.Kvs[i].Key[len(kvc.pfx):]
	}
}

func (kvc *kvPrefixClient) unprefixPutResponse(resp *metaclient.PutResponse) {
}

func (kvc *kvPrefixClient) unprefixDeleteResponse(resp *metaclient.DeleteResponse) {
}

func (kvc *kvPrefixClient) unprefixTxnResponse(resp *metaclient.TxnResponse) {
	for _, r := range resp.Responses {
		switch tv := r.Response.(type) {
		case *metaclient.ResponseOpResponseGet:
			if tv.ResponseGet != nil {
				kvc.unprefixGetResponse(tv.ResponseGet)
			}
		case *metaclient.ResponseOpResponsePut:
			if tv.ResponsePut != nil {
				kvc.unprefixPutResponse(tv.ResponsePut)
			}
		case *metaclient.ResponseOpResponseDelete:
			if tv.ResponseDelete != nil {
				kvc.unprefixDeleteResponse(tv.ResponseDelete)
			}
		case *metaclient.ResponseOpResponseTxn:
			if tv.ResponseTxn != nil {
				kvc.unprefixTxnResponse(tv.ResponseTxn)
			}
		default:
		}
	}
}

func (kvc *kvPrefixClient) prefixInterval(key, end []byte) (pfxKey []byte, pfxEnd []byte) {
	return prefixInterval(kvc.pfx, key, end)
}

func (kvc *kvPrefixClient) prefixOps(ops []metaclient.Op) []metaclient.Op {
	newOps := make([]metaclient.Op, len(ops))
	for i := range ops {
		newOps[i] = kvc.prefixOp(ops[i])
	}
	return newOps
}
