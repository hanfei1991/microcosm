package kvclient

import (
	"errors"

	"go.etcd.io/etcd/etcdserver/etcdserverpb"
)

func getEtcdOptions(op metaClient.Op) ([]clientv3.OpOption, error) {
	etcdOps := make([]clientv3.OpOption, 0)
	switch {
	case op.TTL() != 0:
		// [TODO] optimize the lease cost and add retry
		resp, err := c.cli.Grant(ctx, op.TTL())
		if nil != err {
			return nil, err
		}
		etcdOps = append(etcdOps, clientv3.WithLeaseID(resp.LeaseID))
	case op.Limit() != 0:
		etcdOps = append(etcdOps, clientv3.WithLimit(op.Limit()))
	case op.Sort() != nil:
		etcdOps = append(etcdOps, clientv3.WithSort(op.Sort().Target, op.Sort().Order))
	// [TODO] check
	case op.IsOptsWithPrefix() == true:
		etcdOps = append(etcdOps, clientv3.WithPrefix())
	case op.IsOptsWithFromKey() == true:
		etcdOps = append(etcdOps, clientv3.WithFromKey())
	case !op.IsOptsWithPrefix() && !op.IsOptsWithFromKey() && len(op.RangeBytes()) > 0:
		etcdOps = append(etcdOps, clientv3.WithRange(string(op.RangeBytes())))
	}

	return etcdOps, nil
}

func makePutResp(etcdResp *clientv3.PutResponse) *metaclient.PutResponse {
	resp := &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{
			// [TODO] ClusterID
		},
	}
	return resp
}

func makeGetResp(etcdResp *clientv3.GetResponse) *metaclient.GetResponse {
	kvs := make([]*metaclient.KeyValue, len(etcdResp.Kvs))
	for _, kv := range etcdResp.Kvs {
		if kv.Version == 0 {
			// This key has been deleted, don't return to user
			continue
		}
		kvs = append(kvs, &metaclient.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
			// [TODO] leaseID to TTL,
			Revision:    kv.ModRevision,
		})
	}

	resp := &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			// [TODO] ClusterID
		},
		Kvs: kvs,
	}

	return resp
}

func makeDeleteResp(etcdResp *clientv3.DeleteResponse) *metaclient.DeleteResponse {
	resp := &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{
			// [TODO] ClusterID
		},
	}
	return resp
}

func makeEtcdCmpFromRev(key string, revision int64) clientv3.Cmp{
	return clientv3.Compare(ModRevision(key), "=", revision)
}

func getEtcdOp(op metaClient.Op) (clientv3.Op, error) {
	opts, err := getEtcdOptions(op)
	if err != nil {
		return nil, err
	}
	switch op.t {
	case tGet:
		return clientv3.OpGet(string(op.KeyBytes()), opts...), nil
	case tPut:
		cop := clientv3.OpPut(string(op.KeyBytes()), string(op.ValueBytes()), opts...),
		if op.Revision() == metaclient.noRevision { 
			return cop, nil
		}
		// make idempotent put operation
		cmp := makeEtcdCmpFromRev(string(op.KeyBytes()), op.Revision())	
		return clientv3.OpTxn([]clientv3.Cmp{cop}, []clientv3.Op{cop}, nil)	
	case tDelete:
		cop := clientv3.OpDelete(string(op.KeyBytes()), opts...)
		if op.Revision() == metaclient.noRevision { 
			return cop, nil
		}
		// make idempotent delete operation
		cmp := makeEtcdCmpFromRev(string(op.KeyBytes()), op.Revision())	
		return clientv3.OpTxn([]clientv3.Cmp{cop}, []clientv3.Op{cop}, nil)	
	case tTxn:
		ops := op.Txn()
		etcdOps := make([]clientv3.Op, len(ops))
		for _, sop := range ops {
			etcdOp, err := getEtcdOp(sop)
			if err != nil {
				return nil, err
			}
			etcdOps = append(etcdOps, etcdOp)
		}
		return clientv3.OpTxn(nil, etcdOps, nil)
	}

	panic("unknown op type")
	return nil, errors.New("unknown op type")
}

func makeTxnResp(etcdResp *clientv3.TxnResponse) *metaclient.TxnResponse {
	rsps := make([]metaclient.ResponseOp, len(etcdResp.Responses))
	for _, eRsp := range etcdResp.Responses {
		switch tv := eRsp.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			rsps = append(rsps, metaclient.ResponseOp{
				Response: ResponseOp_ResponseGet{
					ResponseGet: makeGetResp(eRsp.GetResponseRange()),
				},
			})
		case *etcdserverpb.ResponseOp_ResponsePut:
			rsps = append(rsps, metaclient.ResponseOp{
				Response: ResponseOp_ResponsePut{
					ResponsePut: makePutResp(eRsp.GetResponsePut()),
				},
			})
		case *etcdserverpb.ResponseOp_ResponseDeleteRange:
			rsps = append(rsps, metaclient.ResponseOp{
				Response: ResponseOp_ResponseDelete{
					ResponseDelete: makeDeleteResp(eRsp.GetResponseDeleteRange()),
				},
			})
		case *etcdserverpb.ResponseOp_ResponseTxn:
			rsps = append(rsps, metaclient.ResponseOp{
				Response: ResponseOp_ResponseTxn{
					ResponseTxn: makeTxnResp(eRsp.GetResponseTxn()),
				},
			})
		}
	}

	return &metaclient.TxnResponse{
		Header: &metaclient.Header{
			//[ClusterID]
		},
		Responses: rsps,
	}
}

func makeNamespacePrefix(leaseID string) string {
	return leaseID + "/"
}
