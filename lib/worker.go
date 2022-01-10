package lib

import "context"

type Worker interface {
	Init(ctx context.Context) error
	Poll(ctx context.Context) error
	ID() WorkerID
}

