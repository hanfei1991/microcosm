// Copyright 2016 The etcd Authors
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

package metaclient

import "errors"

// [TODO] need check again

type opType int

const (
	// A default Op has opType 0, which is invalid.
	tGet opType = iota + 1
	tPut
	tDelete
	tTxn
)

type SortTarget int
type SortOrder int

const (
	SortNone SortOrder = iota
	SortAscend
	SortDescend
)

const (
	SortByKey SortTarget = iota
)

type SortOption struct {
	Target SortTarget
	Order  SortOrder
}

func (s *SortOption) Clone() *SortOption {
	return &SortOption{
		Target: s.Target,
		Order:  s.Order,
	}
}

var noPrefixEnd = []byte{0}

// [NOTICE]: 0 means no user-specified revision for key.
var noRevision = int64(0)

// Op represents an Operation that kv can execute.
// Support Idempotent/TTL/Key Range/From Key/Key Prefix/Sort/Limit attributes
type Op struct {
	t   opType
	key []byte
	end []byte

	// for idempotent_put/idempotent_delete
	revision int64

	// for range
	limit int64
	sort  *SortOption

	// for put
	val []byte
	ttl int64

	// txn
	ops []Op

	isOptsWithPrefix  bool
	isOptsWithFromKey bool
}

// accessors / mutators

// IsTxn returns true if the "Op" type is transaction.
func (op Op) IsTxn() bool { return op.t == tTxn }

// IsPut returns true if the operation is a Put.
func (op Op) IsPut() bool { return op.t == tPut }

// IsGet returns true if the operation is a Get.
func (op Op) IsGet() bool { return op.t == tGet }

// IsDelete returns true if the operation is a Delete.
func (op Op) IsDelete() bool { return op.t == tDelete }

// IsOptsWithPrefix returns true if WithPrefix option is called in the given opts.
func (op Op) IsOptsWithPrefix() bool { return op.isOptsWithPrefix }

// IsOptsWithFromKey returns true if WithFromKey option is called in the given opts.
func (op Op) IsOptsWithFromKey() bool { return op.isOptsWithFromKey }

// Txn returns the  operations.
func (op Op) Txn() []Op { return op.ops }

// KeyBytes returns the byte slice holding the Op's key.
func (op Op) KeyBytes() []byte { return op.key }

// WithKeyBytes sets the byte slice for the Op's key.
func (op *Op) WithKeyBytes(key []byte) { op.key = key }

// RangeBytes returns the byte slice holding with the Op's range end, if any.
func (op Op) RangeBytes() []byte { return op.end }

// WithRangeBytes sets the byte slice for the Op's range end.
func (op *Op) WithRangeBytes(end []byte) { op.end = end }

// ValueBytes returns the byte slice holding the Op's value, if any.
func (op Op) ValueBytes() []byte { return op.val }

// WithValueBytes sets the byte slice for the Op's value.
func (op *Op) WithValueBytes(v []byte) { op.val = v }

// TTL returns the TTL  holding the Op's key, if any.
func (op Op) TTL() int64 { return op.ttl }

// WithTTL sets the byte slice for the Op's value.
func (op *Op) WithTTL(ttl int64) { op.ttl = ttl }

// Limit returns the limit num for `Get` response, if any.
func (op Op) Limit() int64 { return op.limit }

// WithLimit sets the limit num for `Get` response.
func (op *Op) WithLimit(limit int64) { op.limit = limit }

// Revision returns the revision for key, if any.
func (op Op) Revision() int64 { return op.revision }

// WithRevision sets the revision for key.
func (op *Op) WithRevision(revision int64) { op.revision = revision }

// WithSort sets the sort option for `Get` response.
func (op *Op) WithSort(sort SortOption) { op.sort = sort.Clone() }

// Sort returns the sort option for `Get` response, if any.
func (op Op) Sort() *SortOption {
	if op.sort == nil {
		return nil
	}
	return op.sort.Clone()
}

func NewOp() *Op {
	return &Op{key: []byte("")}
}

// IsOptsWithPrefix returns true if WithPrefix option is called in the given opts.
func IsOptsWithPrefix(opts []OpOption) bool {
	ret := NewOp()
	for _, opt := range opts {
		opt(ret)
	}

	return ret.isOptsWithPrefix
}

// IsOptsWithFromKey returns true if WithFromKey option is called in the given opts.
func IsOptsWithFromKey(opts []OpOption) bool {
	ret := NewOp()
	for _, opt := range opts {
		opt(ret)
	}

	return ret.isOptsWithFromKey
}

// OpGet returns "get" operation based on given key and operation options.
func OpGet(key string, opts ...OpOption) (Op, error) {
	ret := Op{t: tGet, key: []byte(key)}
	ret.ApplyOpts(opts)
	return ret, nil
}

// OpDelete returns "delete" operation based on given key and operation options.
func OpDelete(key string, opts ...OpOption) (Op, error) {
	ret := Op{t: tDelete, key: []byte(key)}
	ret.ApplyOpts(opts)
	switch {
	case ret.sort != nil:
		return *NewOp(), errors.New("unexpected sort option for delete")
	}
	return ret, nil
}

// OpPut returns "put" operation based on given key-value and operation options.
func OpPut(key, val string, opts ...OpOption) (Op, error) {
	ret := Op{t: tPut, key: []byte(key), val: []byte(val)}
	ret.ApplyOpts(opts)
	switch {
	case ret.limit != 0:
		return *NewOp(), errors.New("unexpected limit for put")
	case ret.sort != nil:
		return *NewOp(), errors.New("unexpected sort for put")
	}
	return ret, nil
}

// OpTxn returns "txn" operation based on given transaction conditions.
func OpTxn(ops []Op) (Op, error) {
	return Op{t: tTxn, ops: ops}, nil
}

// GetPrefixRangeEnd gets the range end of the prefix.
// 'Get(foo, WithPrefix())' is equal to 'Get(foo, WithRange(GetPrefixRangeEnd(foo))'.
func GetPrefixRangeEnd(prefix string) string {
	return string(getPrefix([]byte(prefix)))
}

func getPrefix(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i] = end[i] + 1
			end = end[:i+1]
			return end
		}
	}
	// next prefix does not exist (e.g., 0xffff);
	// default to WithFromKey policy
	// [TODO] check
	return noPrefixEnd
}

func (op *Op) ApplyOpts(opts []OpOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// OpOption configures Operations like Get, Put, Delete.
type OpOption func(*Op)

// WithTTL attaches a ttl duration to a key in 'Put' request.
func WithTTL(ttl int64) OpOption { return func(op *Op) { op.ttl = ttl } }

// WithLimit limits the number of results to return from 'Get' request.
// If WithLimit is given a 0 limit, it is treated as no limit.
func WithLimit(n int64) OpOption { return func(op *Op) { op.limit = n } }

// WithSort specifies the ordering in 'Get' request. It requires
// 'WithRange' and/or 'WithPrefix' to be specified too.
// 'target' specifies the target to sort by: key
// 'order' can be either 'SortNone', 'SortAscend', 'SortDescend'.
func WithSort(target SortTarget, order SortOrder) OpOption {
	return func(op *Op) {
		// [TODO] Check params
		op.sort = &SortOption{target, order}
	}
}

// WithPrefix enables 'Get', 'Delete' requests to operate
// on the keys with matching prefix. For example, 'Get(foo, WithPrefix())'
// can return 'foo1', 'foo2', and so on.
func WithPrefix() OpOption {
	return func(op *Op) {
		if len(op.key) == 0 {
			op.key, op.end = []byte{0}, []byte{0}
			return
		}
		op.end = getPrefix(op.key)
		op.isOptsWithPrefix = true
	}
}

// WithRange specifies the range of 'Get', 'Delete' requests.
// For example, 'Get' requests with 'WithRange(end)' returns
// the keys in the range [key, end).
// endKey must be lexicographically greater than start key.
func WithRange(endKey string) OpOption {
	return func(op *Op) { op.end = []byte(endKey) }
}

// WithFromKey specifies the range of 'Get', 'Delete' requests
// to be equal or greater than the key in the argument.
func WithFromKey() OpOption {
	return func(op *Op) {
		if len(op.key) == 0 {
			op.key = []byte{0}
		}
		op.end = []byte("\x00")
		op.isOptsWithFromKey = true
	}
}

// WithRevision specified the revision for 'Put', 'Delete' requests to
// achieve idempotent operation.
// 'Get' requests will always fetch newest value.
func WithRevision(revision int64) OpOption {
	return func(op *Op) { op.revision = revision }
}
