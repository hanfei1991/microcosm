package kvclient

import (
	"context"
)

// KVEx extend the KV interface with Do method to implement the intermediate layer easier
// [NOTICE]: only use inner
type KVEx interface {
	KV

	// Do applies a single Op on KV without a transaction.
	// Do is useful when adding intermidate layer to KV implement
	Do(ctx context.Context, op Op) (OpResponse, Error)
}

// KVClientEx extend the KVClient interface with Do method to implement the intermediate layer easier
// [NOTICE]: only use inner
type KVClientEx interface {
	KVEx
	Client
}
