package kvclient

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
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
			Revision: etcdResp.Header.Revision,
		},
	}
	return resp
}

func makeGetResp(etcdResp *clientv3.GetResponse) *metaclient.GetResponse {
	kvs := make([]*metaclient.KeyValue, len(etcdResp.Kvs))
	for _, kv := range etcdResp.Kvs {
		kvs = append(kvs, &metaclient.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
			// [TODO] leaseID to TTL,
			CreateRevision: kv.CreateRevision,
			ModRevision:    kv.ModRevision,
		})
	}

	resp := &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			// [TODO] ClusterID
			Revision: etcdResp.Header.Revision,
		},
		Kvs: kvs,
	}

	return resp
}

func makeDeleteResp(etcdResp *clientv3.DeleteResponse) *metaclient.DeleteResponse {
	resp := &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{
			// [TODO] ClusterID
			Revision: etcdResp.Header.Revision,
		},
	}
	return resp
}

func getEtcdOp(op metaClient.Op) (clientv3.Op, error) {
	opts, err := getEtcdOptions(op)
	if err != nil {
		return nil, err
	}
	switch {
	case op.IsGet():
		return clientv3.OpGet(string(op.KeyBytes()), opts...), nil
	case op.IsPut():
		return clientv3.OpPut(string(op.KeyBytes()), string(op.ValueBytes()), opts...), nil
	case op.IsDelete():
		return clientv3.OpDelete(string(op.KeyBytes()), opts...), nil
	case op.IsTxn():
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
	for _, eRsp := range etcdRsp.Responses {
		switch reflect.Typeof(eRsp.Response) {
		case etcdserverpb.ResponseOp_ResponseRange:
			rsps = append(rsps, metaclient.ResponseOp{
				Response: ResponseOp_ResponseGet{
					ResponseGet: makeGetResp(eRsp.GetResponseRange()),
				},
			})
		case etcdserverpb.ResponseOp_ResponsePut:
			rsps = append(rsps, metaclient.ResponseOp{
				Response: ResponseOp_ResponsePut{
					ResponsePut: makePutResp(eRsp.GetResponsePut()),
				},
			})
		case etcdserverpb.ResponseOp_ResponseDeleteRange:
			rsps = append(rsps, metaclient.ResponseOp{
				Response: ResponseOp_ResponseDelete{
					ResponseDelete: makeDeleteResp(eRsp.GetResponseDeleteRange()),
				},
			})
		case etcdserverpb.ResponseOp_ResponseTxn:
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
			Revision: etcdResp.Header.Revision,
		},
		Responses: rsps,
	}
}

type etcdKvClient struct {
	cli *clientv3.Client
}

func NewEtcdKVClient(config *metaclient.Config) (*KVClient, error) {
	conf := config.Clone()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            conf.Endpoints,
		DialTimeout:          conf.DialTimeout,
		DialKeepAliveTime:    conf.DialKeepAliveTime,
		DialKeepAliveTimeout: conf.DialKeepAliveTimeout,
		MaxCallSendMsgSize:   conf.MaxSendMsgSize,
		MaxCallRecvMsgSize:   conf.MaxRecvMsgSize,
		// [TODO] TLS
		Username: conf.Auth.Username,
		Password: conf.Auth.Password,
		// [TODO] LOG
	})
	if err != nil {
		return nil, errors.ErrMetaNewClientFail.Wrap(err)
	}

	c := &etcdClient{
		cli: cli,
	}

	return c, nil
}

func (c *etcdKvClient) Put(ctx context.Context, key, val string, opts ...metaclient.OpOption) (*metaclient.PutResponse, error) {
	op, err := OpPut(key, val, opts)
	if err != nil {
		return nil, errors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOps, err := getEtcdOptions(op)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Put(ctx, key, val, etcdOps...)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return makePutResp(etcdResp), nil
}

func (c *etcdKvClient) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, error) {
	op, err := OpGet(key, opts)
	if err != nil {
		return nil, errors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOps, err := getEtcdOptions(op)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Get(ctx, key, etcdOps...)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return makeGetResp(etcdResp), nil
}

func (c *etcdKvClient) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, error) {
	op, err := OpDelete(key, opts)
	if err != nil {
		return nil, errors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOps, err := getEtcdOptions(op)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Delete(ctx, key, etcdOps...)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return makeDeleteResp(etcdResp), nil
}

func (c *etcdKvClient) Txn(ctx context.Context) metaclient.Txn {
	return etcdTxn{
		Txn: c.cli.Txn(ctx),
	}
}

type etcdTxn struct {
	Txn clientv3.Txn
	// cache error to make chain operation work
	Err error
}

func (t *etcdTxn) Do(ops ...metaclient.Op) Txn {
	if t.Err != nil {
		return t
	}
	etcdOps := make([]clientv3.Op, len(ops))
	for _, op := range ops {
		etcdOp, err := getEtcdOp(op)
		if err != nil {
			t.Err = err
			return t
		}
		etcdOps = append(etcdOps, etcdOp)
	}

	t.Txn.Then(etcdOps)
	return t
}

func (t *etcdTxn) Commit(ctx context.Context) (*metaclient.TxnResponse, error) {
	if t.Err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(t.Err)
	}
	etcdResp, err := t.Txn.Commit(ctx)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(t.Err)
	}

	return makeTxnResp(etcdResp), nil
}
