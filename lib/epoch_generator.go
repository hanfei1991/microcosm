package lib

import "context"

type EpochGenerator interface {
	NewEpoch(ctx context.Context) (epoch, error)
}