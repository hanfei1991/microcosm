package kvclient

import (
	"context"
	"github.com/pingcap/errors"
	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"github.com/hanfei1991/microcosm/pkg/metaclient/namespace"
	"go.etcd.io/etcd/clientv3"
)

//[TODO] add retry layer

// etcdKVClient is the implement of kv interface based on etcdClient
// Support namespace isolation and all kv ability
// etcdKVImpl -> kvPrefix -> etcdKVClient
type etcdKVClient struct {
	metaclient.KVClient
	leaseID string
}

func NewEtcdKVClient(config *metaclient.Config, leaseID string) (metaclient.KVClient, error) {
	impl, err := NewEtcdKVImpl(config)
	if err != nil {
		return nil, err
	}

	pfKV := namespace.NewPrefixKV(impl, makeNamespacePrefix(leaseID))
	return &etcdKVClient{
		KVClient: pfKV,
		leaseID:  leaseID,
	}, nil
}

type etcdKVImpl struct {
	cli *clientv3.Client
}

func NewEtcdKVImpl(config *metaclient.Config) (metaclient.KV, error) {
	conf := config.Clone()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:            conf.Endpoints,
		DialTimeout:          conf.Dial.DialTimeout,
		DialKeepAliveTime:    conf.Dial.DialKeepAliveTime,
		DialKeepAliveTimeout: conf.Dial.DialKeepAliveTimeout,
		MaxCallSendMsgSize:   conf.Dial.MaxSendMsgSize,
		MaxCallRecvMsgSize:   conf.Dial.MaxRecvMsgSize,
		// [TODO] TLS
		Username: conf.Auth.Username,
		Password: conf.Auth.Password,
		// [TODO] LOG
	})
	if err != nil {
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	c := &etcdKVImpl{
		cli: cli,
	}
	return c, nil
}

func (c *etcdKVImpl) getEtcdOptions(ctx context.Context, op metaclient.Op) ([]clientv3.OpOption, error) {
	etcdOps := make([]clientv3.OpOption, 0)
	switch {
	case op.TTL() != 0:
		// [TODO] optimize the lease cost and add retry
		resp, err := c.cli.Grant(ctx, op.TTL())
		if nil != err {
			return nil, err
		}
		etcdOps = append(etcdOps, clientv3.WithLease(resp.ID))
	case op.Limit() != 0:
		etcdOps = append(etcdOps, clientv3.WithLimit(op.Limit()))
	case op.Sort() != nil:
		etcdOps = append(etcdOps, clientv3.WithSort((clientv3.SortTarget)(op.Sort().Target), (clientv3.SortOrder)(op.Sort().Order)))
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

var emptyOp = clientv3.Op{}

func (c *etcdKVImpl) getEtcdOp(ctx context.Context, op metaclient.Op) (clientv3.Op, error) {
	opts, err := c.getEtcdOptions(ctx, op)
	if err != nil {
		return emptyOp, err
	}
	switch {
	case op.IsGet():
		return clientv3.OpGet(string(op.KeyBytes()), opts...), nil
	case op.IsPut():
		cop := clientv3.OpPut(string(op.KeyBytes()), string(op.ValueBytes()), opts...)
		if op.Revision() == metaclient.NoRevision {
			return cop, nil
		}
		// make idempotent put operation
		cmp := makeEtcdCmpFromRev(string(op.KeyBytes()), op.Revision())
		return clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{cop}, nil), nil
	case op.IsDelete():
		cop := clientv3.OpDelete(string(op.KeyBytes()), opts...)
		if op.Revision() == metaclient.NoRevision {
			return cop, nil
		}
		// make idempotent delete operation
		cmp := makeEtcdCmpFromRev(string(op.KeyBytes()), op.Revision())
		return clientv3.OpTxn([]clientv3.Cmp{cmp}, []clientv3.Op{cop}, nil), nil
	case op.IsTxn():
		ops := op.Txn()
		etcdOps := make([]clientv3.Op, len(ops))
		for _, sop := range ops {
			etcdOp, err := c.getEtcdOp(ctx, sop)
			if err != nil {
				return emptyOp, err
			}
			etcdOps = append(etcdOps, etcdOp)
		}
		return clientv3.OpTxn(nil, etcdOps, nil), nil
	}

	panic("unknown op type")
	return emptyOp, errors.New("unknown op type")
}

func (c *etcdKVImpl) Put(ctx context.Context, key, val string, opts ...metaclient.OpOption) (*metaclient.PutResponse, error) {
	op, err := metaclient.OpPut(key, val, opts...)
	if err != nil {
		return nil, cerrors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOp, err := c.getEtcdOp(ctx, op)
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return makePutResp(etcdResp.Put()), nil
}

func (c *etcdKVImpl) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, error) {
	op, err := metaclient.OpGet(key, opts...)
	if err != nil {
		return nil, cerrors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOp, err := c.getEtcdOp(ctx, op)
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return makeGetResp(etcdResp.Get()), nil
}

func (c *etcdKVImpl) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, error) {
	op, err := metaclient.OpDelete(key, opts...)
	if err != nil {
		return nil, cerrors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOp, err := c.getEtcdOp(ctx, op)
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return makeDeleteResp(etcdResp.Del()), nil
}

func (c *etcdKVImpl) Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, error) {
	etcdOp, opErr := c.getEtcdOp(ctx, op)
	if opErr != nil {
		return metaclient.OpResponse{}, cerrors.ErrMetaOpFail.Wrap(opErr)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return metaclient.OpResponse{}, cerrors.ErrMetaOpFail.Wrap(opErr)
	}

	switch {
	case op.IsGet():
		return makeGetResp(etcdResp.Get()).OpResponse(), nil
	case op.IsPut():
		return makePutResp(etcdResp.Put()).OpResponse(), nil
	case op.IsDelete():
		return makeDeleteResp(etcdResp.Del()).OpResponse(), nil
	case op.IsTxn():
		return makeTxnResp(etcdResp.Txn()).OpResponse(), nil
	default:
		panic("Unknown op")
	}

	return metaclient.OpResponse{}, nil
}

type etcdTxn struct {
	clientv3.Txn
	kv *etcdKVImpl
	ctx context.Context
	// cache error to make chain operation work
	Err error
}

func (c *etcdKVImpl) Txn(ctx context.Context) metaclient.Txn {
	return &etcdTxn{
		Txn: c.cli.Txn(ctx),
		kv: c,
		ctx: ctx,
	}
}

func (t *etcdTxn) Do(ops ...metaclient.Op) metaclient.Txn {
	if t.Err != nil {
		return t
	}
	etcdOps := make([]clientv3.Op, len(ops))
	for _, op := range ops {
		etcdOp, err := t.kv.getEtcdOp(t.ctx, op)
		if err != nil {
			t.Err = err
			return t
		}
		etcdOps = append(etcdOps, etcdOp)
	}

	t.Txn.Then(etcdOps...)
	return t
}

func (t *etcdTxn) Commit() (*metaclient.TxnResponse, error) {
	if t.Err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(t.Err)
	}
	etcdResp, err := t.Txn.Commit()
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(t.Err)
	}

	return makeTxnResp(etcdResp), nil
}
