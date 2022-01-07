package lib

import "context"

type MasterMetadataFixUpFn = func(ext interface{}) (interface{}, error)

type MasterMetadataManager interface {
	FixUpAndInit(ctx context.Context, fn MasterMetadataFixUpFn) (isInit bool, epoch epoch, err error)
}
