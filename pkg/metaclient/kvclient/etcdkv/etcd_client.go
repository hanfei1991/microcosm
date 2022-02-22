package etcdkv

import (
	"context"
	"sync"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"go.etcd.io/etcd/clientv3"
)

// We will make follow abstracts and implement for KVClient
//				NewEtcdKVClient(KVClient)
//					/			\
//				prefixKV	 etcdClientCloser
//					|			|
//					KV		   Closer
//					\			/
//					   etcdIml(KVClient)

// etcdClientCloser is the etcd implement of Closer interface
type etcdClientCloser struct {
	cli *etcdImpl
}

func (c *etcdClientCloser) Close() error {
	return c.cli.Close()
}

func NewEtcdClientCloser(c *etcdImpl) *etcdClientCloser {
	return &etcdClientCloser{
		cli: c,
	}
}

// etcdImpl is the etcd implement of KVClient interface
// Since we always get the latest data, we will set etcd-server with autocompact parameters
// -auto-compaction-mode: revision
// -auto-compaction-retention: 100
type etcdImpl struct {
	cli     *clientv3.Client
	closeMu sync.Mutex
}

func NewEtcdImpl(config *metaclient.Config) (*etcdImpl, error) {
	conf := config.Clone()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: conf.Endpoints,
		// [TODO] TLS
		// Username: conf.Auth.Username,
		// Password: conf.Auth.Password,
		// [TODO] LOG
	})
	if err != nil {
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	c := &etcdImpl{
		cli: cli,
	}
	return c, nil
}

func (c *etcdImpl) getEtcdOptions(op metaclient.Op) []clientv3.OpOption {
	etcdOps := make([]clientv3.OpOption, 0, 1)
	switch {
	case op.IsOptsWithPrefix():
		etcdOps = append(etcdOps, clientv3.WithPrefix())
	case op.IsOptsWithFromKey():
		etcdOps = append(etcdOps, clientv3.WithFromKey())
	case op.IsOptsWithRange():
		etcdOps = append(etcdOps, clientv3.WithRange(string(op.RangeBytes())))
	}

	return etcdOps
}

func (c *etcdImpl) getEtcdOp(op metaclient.Op) clientv3.Op {
	opts := c.getEtcdOptions(op)
	switch {
	case op.IsGet():
		return clientv3.OpGet(string(op.KeyBytes()), opts...)
	case op.IsPut():
		return clientv3.OpPut(string(op.KeyBytes()), string(op.ValueBytes()), opts...)
	case op.IsDelete():
		return clientv3.OpDelete(string(op.KeyBytes()), opts...)
	case op.IsTxn():
		ops := op.Txn()
		etcdOps := make([]clientv3.Op, 0, len(ops))
		for _, sop := range ops {
			etcdOps = append(etcdOps, c.getEtcdOp(sop))
		}
		return clientv3.OpTxn(nil, etcdOps, nil)
	}

	panic("unknown op type")
}

func (c *etcdImpl) Put(ctx context.Context, key, val string) (*metaclient.PutResponse, error) {
	op := metaclient.OpPut(key, val)
	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(err)
	}

	putRsp := etcdResp.Put()
	return makePutResp(putRsp), nil
}

func (c *etcdImpl) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, error) {
	op := metaclient.OpGet(key, opts...)
	if err := op.CheckValidOp(); err != nil {
		return nil, cerrors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(err)
	}

	getRsp := etcdResp.Get()
	return makeGetResp(getRsp), nil
}

func (c *etcdImpl) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, error) {
	op := metaclient.OpDelete(key, opts...)
	if err := op.CheckValidOp(); err != nil {
		return nil, cerrors.ErrMetaOptionInvalid.Wrap(err)
	}

	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(err)
	}

	delRsp := etcdResp.Del()
	return makeDeleteResp(delRsp), nil
}

func (c *etcdImpl) Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, error) {
	if err := op.CheckValidOp(); err != nil {
		return metaclient.OpResponse{}, err
	}

	etcdResp, err := c.cli.Do(ctx, c.getEtcdOp(op))
	if err != nil {
		return metaclient.OpResponse{}, cerrors.ErrMetaOpFail.GenWithStackByArgs(err)
	}

	switch {
	case op.IsGet():
		rsp := etcdResp.Get()
		getRsp := makeGetResp(rsp)
		return getRsp.OpResponse(), nil
	case op.IsPut():
		rsp := etcdResp.Put()
		putRsp := makePutResp(rsp)
		return putRsp.OpResponse(), nil
	case op.IsDelete():
		rsp := etcdResp.Del()
		delRsp := makeDeleteResp(rsp)
		return delRsp.OpResponse(), nil
	case op.IsTxn():
		rsp := etcdResp.Txn()
		txnRsp := makeTxnResp(rsp)
		return txnRsp.OpResponse(), nil
	default:
		panic("Unknown op")
	}
}

type etcdTxn struct {
	clientv3.Txn

	mu  sync.Mutex
	kv  *etcdImpl
	ops []clientv3.Op
	// cache error to make chain operation work
	Err       error
	committed bool
}

func (c *etcdImpl) Txn(ctx context.Context) metaclient.Txn {
	return &etcdTxn{
		Txn: c.cli.Txn(ctx),
		kv:  c,
		ops: make([]clientv3.Op, 0, 3),
	}
}

func (c *etcdImpl) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.cli != nil {
		err := c.cli.Close()
		c.cli = nil
		return err
	}

	return nil
}

func (t *etcdTxn) Do(ops ...metaclient.Op) metaclient.Txn {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Err != nil {
		return t
	}
	if t.committed {
		t.Err = cerrors.ErrMetaCommittedTxn.GenWithStackByArgs()
		return t
	}

	etcdOps := make([]clientv3.Op, 0, len(ops))
	for _, op := range ops {
		if op.IsTxn() {
			t.Err = cerrors.ErrMetaNestedTxn.GenWithStackByArgs()
			return t
		}
		etcdOps = append(etcdOps, t.kv.getEtcdOp(op))
	}

	t.ops = append(t.ops, etcdOps...)
	return t
}

func (t *etcdTxn) Commit() (*metaclient.TxnResponse, error) {
	t.mu.Lock()
	if t.Err != nil {
		t.mu.Unlock()
		return nil, t.Err
	}
	if t.committed {
		t.Err = cerrors.ErrMetaCommittedTxn.GenWithStackByArgs()
		t.mu.Unlock()
		return nil, t.Err
	}
	t.committed = true
	t.mu.Unlock()

	t.Txn.Then(t.ops...)
	etcdResp, err := t.Txn.Commit()
	if err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(err)
	}

	return makeTxnResp(etcdResp), nil
}
