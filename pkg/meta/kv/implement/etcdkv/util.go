package etcdkv

import (
	"strconv"

	"github.com/pingcap/tiflow/pkg/errorutil"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
)

func makePutResp(etcdResp *clientv3.PutResponse) *kvclient.PutResponse {
	resp := &kvclient.PutResponse{
		Header: &kvclient.ResponseHeader{
			// [TODO] use another ClusterID
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
	}

	return resp
}

func makeGetResp(etcdResp *clientv3.GetResponse) *kvclient.GetResponse {
	kvs := make([]*kvclient.KeyValue, 0, len(etcdResp.Kvs))
	for _, kv := range etcdResp.Kvs {
		kvs = append(kvs, &kvclient.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}
	resp := &kvclient.GetResponse{
		Header: &kvclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
		Kvs: kvs,
	}

	return resp
}

func makeDeleteResp(etcdResp *clientv3.DeleteResponse) *kvclient.DeleteResponse {
	resp := &kvclient.DeleteResponse{
		Header: &kvclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
	}

	return resp
}

func makeTxnResp(etcdResp *clientv3.TxnResponse) *kvclient.TxnResponse {
	rsps := make([]kvclient.ResponseOp, 0, len(etcdResp.Responses))
	for _, eRsp := range etcdResp.Responses {
		switch eRsp.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			getRsp := makeGetResp((*clientv3.GetResponse)(eRsp.GetResponseRange()))
			rsps = append(rsps, kvclient.ResponseOp{
				Response: &kvclient.ResponseOpResponseGet{
					ResponseGet: getRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponsePut:
			putRsp := makePutResp((*clientv3.PutResponse)(eRsp.GetResponsePut()))
			rsps = append(rsps, kvclient.ResponseOp{
				Response: &kvclient.ResponseOpResponsePut{
					ResponsePut: putRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponseDeleteRange:
			delRsp := makeDeleteResp((*clientv3.DeleteResponse)(eRsp.GetResponseDeleteRange()))
			rsps = append(rsps, kvclient.ResponseOp{
				Response: &kvclient.ResponseOpResponseDelete{
					ResponseDelete: delRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponseTxn:
			panic("unexpected nested txn")
		}
	}

	return &kvclient.TxnResponse{
		Header: &kvclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
		Responses: rsps,
	}
}

// etcdError wraps IsRetryable to etcd error.
type etcdError struct {
	displayed error
	cause     error
}

func (e *etcdError) IsRetryable() bool {
	if e.cause != nil {
		return errorutil.IsRetryableEtcdError(e.cause)
	}
	// currently all retryable errors are etcd errors
	return false
}

func (e *etcdError) Error() string {
	return e.displayed.Error()
}

func etcdErrorFromOpFail(err error) *etcdError {
	return &etcdError{
		cause:     err,
		displayed: cerrors.ErrMetaOpFail.GenWithStackByArgs(err),
	}
}
