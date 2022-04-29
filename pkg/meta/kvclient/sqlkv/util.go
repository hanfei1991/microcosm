package sqlkv

import (
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/pkg/errorutil"
)

func makeGetResponse(kvs []*KeyValue) *metaclient.GetResponse {
	return &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			//TODO: clusterID
		},
		Kvs: kvs,
	}
}

// sqlError wraps IsRetryable to etcd error.
type sqlError struct {
	displayed error
	cause     error
}

func (e *sqlError) IsRetryable() bool {
	if e.cause != nil {
		return errorutil.IsRetryableEtcdError(e.cause)
	}
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
