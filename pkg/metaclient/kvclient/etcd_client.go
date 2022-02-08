package kvclient

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"github.com/hanfei1991/microcosm/pkg/metaclient/namespace"
	"go.etcd.io/etcd/clientv3"
)

//[TODO] add retry logic

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
		metaclient.KVClient: pfKV,
		leaseID:             leaseID,
	}, nil
}

type etcdKVImpl struct {
	cli *clientv3.Client
}

func NewEtcdKVImpl(config *metaclient.Config) (metaclient.KV, error) {
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

	c := &etcdKVImpl{
		cli: cli,
	}

	return c, nil
}

func (c *etcdKVImpl) Put(ctx context.Context, key, val string, opts ...metaclient.OpOption) (*metaclient.PutResponse, error) {
	op, err := OpPut(key, val, opts)
	if err != nil {
		return nil, errors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOp, err := getEtcdOp(op)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return makePutResp(etcdResp.Put()), nil
}

func (c *etcdKVImpl) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, error) {
	op, err := OpGet(key, opts)
	if err != nil {
		return nil, errors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOp, err := getEtcdOp(op)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return makeGetResp(etcdResp.Get()), nil
}

func (c *etcdKVImpl) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, error) {
	op, err := OpDelete(key, opts)
	if err != nil {
		return nil, errors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdOp, err := getEtcdOp(op)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return makeDeleteResp(etcdResp.Delete()), nil
}

func (c *etcdKVImpl) Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, error) {
	etcdOp, opErr := getEtcdOp(op)
	if opErr != nil {
		return metaclient.OpResponse{}, cerrors.ErrMetaOpFail.Wrap(opErr)
	}

	etcdResp, err := c.cli.Do(ctx, etcdOp)
	if err != nil {
		return metaclient.OpResponse{}, cerrors.ErrMetaOpFail.Wrap(opErr)
	}

	switch op.t {
	case tGet:
		return metaclient.OpResponse{get: makeGetResp(etcdResp.Get())}, nil
	case tPut:
		return metaclient.OpResponse{put: makePutResp(etcdResp.Put())}, nil
	case tDelete:
		return metaclient.OpResponse{del: makeDeleteResp(etcdResp.Delete())}, nil
	case tTxn:
		return metaclient.OpResponse{txn: makeTxnResp(etcdResp.Txn())}, nil
	default:
		panic("Unknown op")
	}

	return metaclient.OpResponse{}, nil
}

type etcdTxn struct {
	clientv3.Txn
	// cache error to make chain operation work
	Err error
}

func (c *etcdKVImpl) Txn(ctx context.Context) metaclient.Txn {
	return &etcdTxn{
		clientv3.Txn: c.cli.Txn(ctx),
	}
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
	etcdResp, err := t.Txn.Commit()
	if err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(t.Err)
	}

	return makeTxnResp(etcdResp), nil
}
