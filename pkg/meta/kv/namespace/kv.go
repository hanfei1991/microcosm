// Copyright 2022 PingCAP, Inc.
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

//[reference]: https://github.com/etcd-io/etcd/blob/aa75fd08509db3aea8939cdad44e1ee9b8157b8c/client/v3/namespace/kv.go

package namespace

import (
	"context"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
)

type prefixError struct {
	cause error
}

func (p *prefixError) IsRetryable() bool {
	return false
}

func (p *prefixError) Error() string {
	return p.cause.Error()
}

type kvPrefix struct {
	kvclient.KVEx
	pfx string
}

// NewPrefixKV wraps a KVEx instance so that all requests
// are prefixed with a given string.
func NewPrefixKV(kv kvclient.KVEx, prefix string) kvclient.KV {
	return &kvPrefix{kv, prefix}
}

func (kv *kvPrefix) Put(ctx context.Context, key, val string) (*kvclient.PutResponse, kvclient.Error) {
	if len(key) == 0 {
		return nil, prefixErrorFromOpFail(cerrors.ErrMetaEmptyKey.GenWithStackByArgs())
	}
	op := kv.prefixOp(kvclient.OpPut(key, val))
	r, err := kv.KVEx.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	put := r.Put()
	return put, nil
}

func (kv *kvPrefix) Get(ctx context.Context, key string, opts ...kvclient.OpOption) (*kvclient.GetResponse, kvclient.Error) {
	// Forbid empty key to protect the namespace prefix key
	if len(key) == 0 && !(kvclient.IsOptsWithFromKey(opts) || kvclient.IsOptsWithPrefix(opts) || kvclient.IsOptsWithRange(opts)) {
		return nil, prefixErrorFromOpFail(cerrors.ErrMetaEmptyKey.GenWithStackByArgs())
	}
	r, err := kv.KVEx.Do(ctx, kv.prefixOp(kvclient.OpGet(key, opts...)))
	if err != nil {
		return nil, err
	}
	get := r.Get()
	kv.unprefixGetResponse(get)
	return get, nil
}

func (kv *kvPrefix) Delete(ctx context.Context, key string, opts ...kvclient.OpOption) (*kvclient.DeleteResponse, kvclient.Error) {
	// Forbid empty key to protect the namespace prefix key
	if len(key) == 0 && !(kvclient.IsOptsWithFromKey(opts) || kvclient.IsOptsWithPrefix(opts) || kvclient.IsOptsWithRange(opts)) {
		return nil, prefixErrorFromOpFail(cerrors.ErrMetaEmptyKey.GenWithStackByArgs())
	}
	r, err := kv.KVEx.Do(ctx, kv.prefixOp(kvclient.OpDelete(key, opts...)))
	if err != nil {
		return nil, err
	}
	del := r.Del()
	return del, nil
}

// [TODO] check the empty key
func (kv *kvPrefix) Do(ctx context.Context, op kvclient.Op) (kvclient.OpResponse, kvclient.Error) {
	if len(op.KeyBytes()) == 0 && !op.IsTxn() {
		return kvclient.OpResponse{}, prefixErrorFromOpFail(cerrors.ErrMetaEmptyKey.GenWithStackByArgs())
	}
	r, err := kv.KVEx.Do(ctx, kv.prefixOp(op))
	if err != nil {
		return r, err
	}
	switch {
	case r.Get() != nil:
		kv.unprefixGetResponse(r.Get())
	case r.Txn() != nil:
		kv.unprefixTxnResponse(r.Txn())
	}
	return r, nil
}

type txnPrefix struct {
	kvclient.Txn
	kv *kvPrefix
}

func (kv *kvPrefix) Txn(ctx context.Context) kvclient.Txn {
	return &txnPrefix{kv.KVEx.Txn(ctx), kv}
}

// [TODO] check the empty key
func (txn *txnPrefix) Do(ops ...kvclient.Op) kvclient.Txn {
	txn.Txn = txn.Txn.Do(txn.kv.prefixOps(ops)...)
	return txn
}

func (txn *txnPrefix) Commit() (*kvclient.TxnResponse, kvclient.Error) {
	resp, err := txn.Txn.Commit()
	if err != nil {
		return nil, err
	}
	txn.kv.unprefixTxnResponse(resp)
	return resp, nil
}

func (kv *kvPrefix) prefixOp(op kvclient.Op) kvclient.Op {
	if !op.IsTxn() {
		begin, end := kv.prefixInterval(op.KeyBytes(), op.RangeBytes())
		op.WithKeyBytes(begin)
		op.WithRangeBytes(end)
		return op
	}
	return kvclient.OpTxn(kv.prefixOps(op.Txn()))
}

func (kv *kvPrefix) unprefixGetResponse(resp *kvclient.GetResponse) {
	for i := range resp.Kvs {
		resp.Kvs[i].Key = resp.Kvs[i].Key[len(kv.pfx):]
	}
}

func (kv *kvPrefix) unprefixTxnResponse(resp *kvclient.TxnResponse) {
	for _, r := range resp.Responses {
		switch tv := r.Response.(type) {
		case *kvclient.ResponseOpResponseGet:
			if tv.ResponseGet != nil {
				kv.unprefixGetResponse(tv.ResponseGet)
			}
		case *kvclient.ResponseOpResponseTxn:
			if tv.ResponseTxn != nil {
				kv.unprefixTxnResponse(tv.ResponseTxn)
			}
		default:
		}
	}
}

func (kv *kvPrefix) prefixInterval(key, end []byte) (pfxKey []byte, pfxEnd []byte) {
	return prefixInterval(kv.pfx, key, end)
}

func (kv *kvPrefix) prefixOps(ops []kvclient.Op) []kvclient.Op {
	newOps := make([]kvclient.Op, len(ops))
	for i := range ops {
		newOps[i] = kv.prefixOp(ops[i])
	}
	return newOps
}
