package kvclient

import "context"

type Client interface {
	// Close is the method to close the client and release inner resources
	Close() error

	// GenEpoch generate the increasing epoch for user
	GenEpoch(ctx context.Context) (int64, error)
}

// KVClient is user interface for kvclient
type KVClient interface {
	Client
	KV
}
