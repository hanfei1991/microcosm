package kvclient

import (
	"strconv"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
)

func makePutResp(etcdResp clientv3.OpResponse, isTxn bool) (*metaclient.PutResponse, error) {
	var header *etcdserverpb.ResponseHeader
	if isTxn {
		txnRsp := etcdResp.Txn()
		// revision is not expected
		if !txnRsp.Succeeded {
			return nil, cerrors.ErrMetaRevisionUnmatch
		}
		header = txnRsp.Header
	} else {
		header = etcdResp.Put().Header
	}
	resp := &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{
			// [TODO] use another ClusterID
			ClusterID: strconv.FormatUint(header.ClusterId, 10),
		},
	}

	return resp, nil
}

func makeGetResp(etcdResp *clientv3.GetResponse) (*metaclient.GetResponse, error) {
	kvs := make([]*metaclient.KeyValue, 0, len(etcdResp.Kvs))
	for _, kv := range etcdResp.Kvs {
		if kv.Version == 0 {
			// This key has been deleted, don't return to user
			continue
		}
		kvs = append(kvs, &metaclient.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
			// [TODO] leaseID to TTL,
			Revision: kv.ModRevision,
		})
	}
	resp := &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
		Kvs: kvs,
	}

	return resp, nil
}

func makeDeleteResp(etcdResp clientv3.OpResponse, isTxn bool) (*metaclient.DeleteResponse, error) {
	var header *etcdserverpb.ResponseHeader
	if isTxn {
		txnRsp := etcdResp.Txn()
		// revision is not expected
		if !txnRsp.Succeeded {
			return nil, cerrors.ErrMetaRevisionUnmatch
		}
		header = txnRsp.Header
	} else {
		header = etcdResp.Del().Header
	}
	resp := &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: strconv.FormatUint(header.ClusterId, 10),
		},
	}

	return resp, nil
}

func makeEtcdCmpFromRev(key string, revision int64) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", revision)
}

func makeTxnResp(etcdResp *clientv3.TxnResponse) (*metaclient.TxnResponse, error) {
	// revision is not expected
	if !etcdResp.Succeeded {
		return nil, cerrors.ErrMetaRevisionUnmatch
	}
	rsps := make([]metaclient.ResponseOp, 0, len(etcdResp.Responses))
	for _, eRsp := range etcdResp.Responses {
		switch eRsp.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			getRsp, _ := makeGetResp((*clientv3.GetResponse)(eRsp.GetResponseRange()))
			rsps = append(rsps, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponseGet{
					ResponseGet: getRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponsePut:
			putRsp, _ := makePutResp((*clientv3.PutResponse)(eRsp.GetResponsePut()).OpResponse(), false /*IsTxn*/)
			rsps = append(rsps, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponsePut{
					ResponsePut: putRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponseDeleteRange:
			delRsp, _ := makeDeleteResp((*clientv3.DeleteResponse)(eRsp.GetResponseDeleteRange()).OpResponse(), false /*IsTxn*/)
			rsps = append(rsps, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponseDelete{
					ResponseDelete: delRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponseTxn:
			panic("unexpected nested txn")
		}
	}

	return &metaclient.TxnResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
		Responses: rsps,
	}, nil
}

func makeNamespacePrefix(leaseID string) string {
	return leaseID + "/"
}
