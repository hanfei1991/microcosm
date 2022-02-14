package metaclient

import "context"

// Txn doesn't support nested txn
type Txn interface {
	// Do cache Ops in the Txn
	// Same op limit with KV Put/Get/Delete interface
	// Using snapshot isolation
	Do(ops ...Op) Txn

	// Commit tries to commit the transaction.
	// Any Op fail will cause entire txn rollback and return error
	Commit() (*TxnResponse, error)
}

type KV interface {
	// Put puts a key-value pair into metastore.
	// Note that key,value can be plain bytes array and string is
	// an immutable representation of that bytes array.
	// To get a string of bytes, do string([]byte{0x10, 0x20}).
	// or do nothing on vice verse.
	Put(ctx context.Context, key, val string) (*PutResponse, error)

	// Get retrieves keys with newest revision.
	// By default, Get will return the value for "key", if any.
	// When WithRange(end) is passed, Get will return the keys in the range [key, end).
	// When WithFromKey() is passed, Get returns keys greater than or equal to key.
	// When WithPrefix() is passed, Get returns keys with prefix.
	// WARN: WithRange(), WithFromKey(), WithPrefix() can't be used at the same time
	Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error)

	// Delete deletes a key, or optionally using WithRange(end), [key, end).
	// WARN: WithRange(end), WithFromKey(), WithPrefix() can't be used at the same time
	Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error)

	// Txn creates a transaction.
	Txn(ctx context.Context) Txn
}
