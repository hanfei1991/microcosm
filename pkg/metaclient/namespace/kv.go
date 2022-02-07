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

type kvPrefix struct {
	metaclient.KV
	pfx string
}

// NewPrefixKV wraps a KV instance so that all requests
// are prefixed with a given string.
func NewPrefixKV(kv metaclient.KV, prefix string) metaclient.KV {
	return &kvPrefix{kv, prefix}
}

func (kv *kvPrefix) Put(ctx context.Context, key, val string, opts ...metaclient.OpOption) (*metaclient.PutResponse, error) {
	if len(key) == 0 {
		return nil, cerrors.ErrMetaEmptyKey
	}
	orgOp, err := metaclient.OpPut(key, val, opts...)
	if err != nil {
		return nil, err
	}
	op := kv.prefixOp(orgOp)
	r, err := kv.KV.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	put := r.Put()
	kv.unprefixPutResponse(put)
	return put, nil
}

func (kv *kvPrefix) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, error) {
	if len(key) == 0 && !(metaclient.IsOptsWithFromKey(opts) || metaclient.IsOptsWithPrefix(opts)) {
		return nil, cerrors.ErrMetaEmptyKey
	}
	orgOp, err := metaclient.OpGet(key, opts...)
	if err != nil {
		return nil, err
	}
	r, err := kv.KV.Do(ctx, kv.prefixOp(orgOp))
	if err != nil {
		return nil, err
	}
	get := r.Get()
	kv.unprefixGetResponse(get)
	return get, nil
}

func (kv *kvPrefix) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, error) {
	if len(key) == 0 && !(metaclient.IsOptsWithFromKey(opts) || metaclient.IsOptsWithPrefix(opts)) {
		return nil, cerrors.ErrMetaEmptyKey
	}
	orgOp, err := metaclient.OpDelete(key, opts...)
	if err != nil {
		return nil, err
	}
	r, err := kv.KV.Do(ctx, kv.prefixOp(orgOp))
	if err != nil {
		return nil, err
	}
	del := r.Del()
	kv.unprefixDeleteResponse(del)
	return del, nil
}

func (kv *kvPrefix) Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, error) {
	if len(op.KeyBytes()) == 0 && !op.IsTxn() {
		return metaclient.OpResponse{}, cerrors.ErrMetaEmptyKey
	}
	r, err := kv.KV.Do(ctx, kv.prefixOp(op))
	if err != nil {
		return r, err
	}
	switch {
	case r.Get() != nil:
		kv.unprefixGetResponse(r.Get())
	case r.Put() != nil:
		kv.unprefixPutResponse(r.Put())
	case r.Del() != nil:
		kv.unprefixDeleteResponse(r.Del())
	case r.Txn() != nil:
		kv.unprefixTxnResponse(r.Txn())
	}
	return r, nil
}

type txnPrefix struct {
	metaclient.Txn
	kv *kvPrefix
}

func (kv *kvPrefix) Txn(ctx context.Context) metaclient.Txn {
	return &txnPrefix{kv.KV.Txn(ctx), kv}
}

func (txn *txnPrefix) Do(ops ...metaclient.Op) metaclient.Txn {
	txn.Txn = txn.Txn.Do(txn.kv.prefixOps(ops)...)
	return txn
}

func (txn *txnPrefix) Commit(ctx context.Context) (*metaclient.TxnResponse, error) {
	resp, err := txn.Txn.Commit(ctx)
	if err != nil {
		return nil, err
	}
	txn.kv.unprefixTxnResponse(resp)
	return resp, nil
}

func (kv *kvPrefix) prefixOp(op metaclient.Op) metaclient.Op {
	if !op.IsTxn() {
		begin, end := kv.prefixInterval(op.KeyBytes(), op.RangeBytes())
		op.WithKeyBytes(begin)
		op.WithRangeBytes(end)
		return op
	}
	op, err := metaclient.OpTxn(kv.prefixOps(op.Txn()))
	if err != nil {
		panic("unreachable b")
	}
	return op
}

func (kv *kvPrefix) unprefixGetResponse(resp *metaclient.GetResponse) {
	for i := range resp.Kvs {
		resp.Kvs[i].Key = resp.Kvs[i].Key[len(kv.pfx):]
	}
}

func (kv *kvPrefix) unprefixPutResponse(resp *metaclient.PutResponse) {
}

func (kv *kvPrefix) unprefixDeleteResponse(resp *metaclient.DeleteResponse) {
}

func (kv *kvPrefix) unprefixTxnResponse(resp *metaclient.TxnResponse) {
	for _, r := range resp.Responses {
		switch tv := r.Response.(type) {
		case *metaclient.ResponseOp_ResponseGet:
			if tv.ResponseGet != nil {
				kv.unprefixGetResponse((*metaclient.GetResponse)(tv.ResponseGet))
			}
		case *metaclient.ResponseOp_ResponsePut:
			if tv.ResponsePut != nil {
				kv.unprefixPutResponse((*metaclient.PutResponse)(tv.ResponsePut))
			}
		case *metaclient.ResponseOp_ResponseDelete:
			if tv.ResponseDelete != nil {
				kv.unprefixDeleteResponse((*metaclient.DeleteResponse)(tv.ResponseDelete))
			}
		case *metaclient.ResponseOp_ResponseTxn:
			if tv.ResponseTxn != nil {
				kv.unprefixTxnResponse((*metaclient.TxnResponse)(tv.ResponseTxn))
			}
		default:
		}
	}
}

func (kv *kvPrefix) prefixInterval(key, end []byte) (pfxKey []byte, pfxEnd []byte) {
	return prefixInterval(kv.pfx, key, end)
}

func (kv *kvPrefix) prefixOps(ops []metaclient.Op) []metaclient.Op {
	newOps := make([]metaclient.Op, len(ops))
	for i := range ops {
		newOps[i] = kv.prefixOp(ops[i])
	}
	return newOps
}
