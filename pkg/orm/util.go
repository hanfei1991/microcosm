package orm

import (
	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	perrors "github.com/pingcap/errors"
)

// TODO: refine me, need wrap error for api
func IsNotFoundError(err error) bool {
	return err.(*perrors.Error).Is(cerrors.ErrMetaEntryNotFound)
}
