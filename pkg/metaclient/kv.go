package metaclient

import "context"

type Txn interface {
	// Do cache Ops in the Txn
	Do(ops ...Op) Txn

	// Commit tries to commit the transaction.
	// Any Op error will cause entire txn rollback and return error
	Commit() (*TxnResponse, error)
}

type KV interface {
	// Put puts a key-value pair into metastore.
	// Note that key,value can be plain bytes array and string is
	// an immutable representation of that bytes array.
	// To get a string of bytes, do string([]byte{0x10, 0x20}).
	// When passed WithTTL(ttl), Put will insert a key with ttl duration.
	// When passed WithRevision(revision), Put will be executed with corresponding revision,
	// or do nothing on vice verse.
	Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error)

	// Get retrieves keys with newest revision.
	// By default, Get will return the value for "key", if any.
	// When passed WithRange(end), Get will return the keys in the range [key, end).
	// When passed WithFromKey(), Get returns keys greater than or equal to key.
	// When passed WithPrefix(), Get returns keys with prefix.
	// When passed WithLimit(limit), the number of returned keys is bounded by limit.
	// When passed WithSort(), the keys will be sorted.
	Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error)

	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	// When passed WithRevision(revision), Put will be executed with corresponding revision
	// [TODO] How to support idempotent range delete???
	Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error)

	// Do applies a single Op on KV without a transaction.
	// Do is useful when creating arbitrary operations to be issued at a
	// later time and making intermediate layer for kv; the user can range over the operations, calling Do to
	// execute them. Get/Put/Delete, on the other hand, are best suited
	// for when the operation should be issued at the time of declaration.
	Do(ctx context.Context, op Op) (OpResponse, error)

	// Txn creates a transaction.
	Txn(ctx context.Context) Txn

	// [TODO] Add a high-level txn interface to support CAS txn
}
