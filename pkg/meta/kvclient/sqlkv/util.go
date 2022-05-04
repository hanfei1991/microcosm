package sqlkv

import (
	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/pkg/errorutil"
)

// sqlError wraps IsRetryable to etcd error.
type sqlError struct {
	displayed error
	cause     error
}

func (e *sqlError) IsRetryable() bool {
	if e.cause != nil {
		return errorutil.IsRetryableEtcdError(e.cause)
	}
	// TODO define retryable error
	return false
}

func (e *sqlError) Error() string {
	return e.displayed.Error()
}

func sqlErrorFromOpFail(err error) *sqlError {
	return &sqlError{
		cause:     err,
		displayed: cerrors.ErrMetaOpFail.GenWithStackByArgs(err),
	}
}

func makeGetResponseOp(rsp *metaclient.GetResponse) metaclient.ResponseOp {
	return metaclient.ResponseOp{
		Response: &metaclient.ResponseOpResponseGet{
			ResponseGet: rsp,
		},
	}
}

func makePutResponseOp(rsp *metaclient.PutResponse) metaclient.ResponseOp {
	return metaclient.ResponseOp{
		Response: &metaclient.ResponseOpResponsePut{
			ResponsePut: rsp,
		},
	}
}

func makeDelResponseOp(rsp *metaclient.DeleteResponse) metaclient.ResponseOp {
	return metaclient.ResponseOp{
		Response: &metaclient.ResponseOpResponseDelete{
			ResponseDelete: rsp,
		},
	}
}
