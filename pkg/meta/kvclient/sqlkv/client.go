package sqlkv

import (
	"context"
	"database/sql"
	"sync"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/sqlkv/model"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/sqlutil"
	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var globalModels = []interface{}{
	&metaclient.KeyValue{},
	&model.LogicEpoch{},
}

// sqlImpl is the mysql-compatiable implement for KVClient
type sqlImpl struct {
	// gorm claim to be thread safe
	db   *gorm.DB
	impl *sql.DB
}

// TODO: interface definition and isolation
func NewSQLImpl(mc *metaclient.StoreConfigParams, projectID tenant.ProjectID, sqlConf sqlutil.DBConfig) (*sqlImpl, error) {
	err := sqlutil.CreateDatabaseForProject(*mc, projectID, sqlConf)
	if err != nil {
		return nil, err
	}

	dsn := sqlutil.GenerateDSNByParams(*mc, projectID, sqlConf, true)
	sqlDB, err := sqlutil.NewSQLDB("mysql", dsn, sqlConf)
	if err != nil {
		return nil, err
	}

	cli, err := newImpl(sqlDB)
	if err != nil {
		sqlDB.Close()
	}

	return cli, err
}

func newImpl(sqlDB *sql.DB) (*sqlImpl, error) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: false,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		log.L().Error("create gorm client fail", zap.Error(err))
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	return &sqlImpl{
		db:   db,
		impl: sqlDB,
	}, nil
}

func (c *sqlImpl) Close() error {
	if c.impl != nil {
		return c.impl.Close()
	}

	return nil
}

// Initialize will create all related tables in SQL backend
// TODO: What if we change the definition of orm??
func (c *sqlImpl) Initialize(ctx context.Context) error {
	if err := c.db.AutoMigrate(globalModels); err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	// check first record in logic_epochs
	return model.InitializeEpoch(ctx, c.db)
}

func (c *sqlImpl) GenEpoch(ctx context.Context) (int64, error) {
	return model.GenEpoch(ctx, c.db)
}

func (c *sqlImpl) Put(ctx context.Context, key, val string) (*metaclient.PutResponse, metaclient.Error) {
	op := metaclient.OpPut(key, val)
	return c.doPut(ctx, c.db, &op)
}

func (c *sqlImpl) doPut(ctx context.Context, db *gorm.DB, op *metaclient.Op) (*metaclient.PutResponse, metaclient.Error) {
	// TODO:
	sql := "REPLACE INTO `key_values`(`key`, `value`) VALUES(?, ?)"
	if err := db.Exec(sql, op.KeyBytes(), op.ValueBytes()).Error; err != nil {
		return nil, sqlErrorFromOpFail(err)
	}

	return &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{
			//TODO: clusterID
		},
	}, nil
}

func (c *sqlImpl) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, metaclient.Error) {
	op := metaclient.OpGet(key, opts...)
	return c.doGet(ctx, c.db, &op)
}

func (c *sqlImpl) doGet(ctx context.Context, db *gorm.DB, op *metaclient.Op) (*metaclient.GetResponse, metaclient.Error) {
	if err := op.CheckValidOp(); err != nil {
		return nil, &sqlError{
			displayed: cerrors.ErrMetaOptionInvalid.Wrap(err),
		}
	}

	var (
		kvs        []*metaclient.KeyValue
		kv         metaclient.KeyValue
		err        error
		isPointGet bool
		key        = op.KeyBytes()
	)
	// TODO: need deal special range for key
	if op.IsOptsWithRange() {
		err = db.Where("key >= ? && key < ?", key, op.RangeBytes()).Find(&kvs).Error
	} else if op.IsOptsWithPrefix() {
		err = db.Where("key like ?%", key).Find(&kvs).Error
	} else if op.IsOptsWithFromKey() {
		err = db.Where("key >= ?", key).Find(&kvs).Error
	} else {
		// TODO optimize
		err = db.Where("key = ?", key).First(&kv).Error
		isPointGet = true
	}
	if err != nil {
		return nil, sqlErrorFromOpFail(err)
	}

	if isPointGet {
		kvs = make([]*metaclient.KeyValue, 0, 1)
		kvs = append(kvs, &kv)
	}

	return &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			//TODO: clusterID
		},
		Kvs: kvs,
	}, nil
}

func (c *sqlImpl) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, metaclient.Error) {
	op := metaclient.OpDelete(key, opts...)
	return c.doDelete(ctx, c.db, &op)
}

func (c *sqlImpl) doDelete(ctx context.Context, db *gorm.DB, op *metaclient.Op) (*metaclient.DeleteResponse, metaclient.Error) {
	if err := op.CheckValidOp(); err != nil {
		return nil, &sqlError{
			displayed: cerrors.ErrMetaOptionInvalid.Wrap(err),
		}
	}

	var (
		err error
		key = op.KeyBytes()
	)
	// TODO: need deal special range for key
	if op.IsOptsWithRange() {
		err = db.Where("key >= ? && key < ?", key,
			op.RangeBytes()).Delete(&metaclient.KeyValue{}).Error
	} else if op.IsOptsWithPrefix() {
		err = db.Where("key like ?%", key).Delete(&metaclient.KeyValue{}).Error
	} else if op.IsOptsWithFromKey() {
		err = db.Where("key >= ?", key).Delete(&metaclient.KeyValue{}).Error
	} else {
		err = db.Where("key = ?", key).Delete(&metaclient.KeyValue{}).Error
	}

	if err != nil {
		return nil, sqlErrorFromOpFail(err)
	}

	return &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{
			// TODO:
		},
	}, nil
}

type sqlTxn struct {
	mu sync.Mutex

	ctx  context.Context
	impl *sqlImpl
	ops  []metaclient.Op
	// cache error to make chain operation work
	Err       *sqlError
	committed bool
}

func (c *sqlImpl) Txn(ctx context.Context) metaclient.Txn {
	return &sqlTxn{
		ctx:  ctx,
		impl: c,
		ops:  make([]metaclient.Op, 0, 2),
	}
}

func (t *sqlTxn) Do(ops ...metaclient.Op) metaclient.Txn {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.Err != nil {
		return t
	}

	if t.committed {
		t.Err = &sqlError{
			displayed: cerrors.ErrMetaCommittedTxn.GenWithStackByArgs("txn had been committed"),
		}
		return t
	}

	t.ops = append(t.ops, ops...)
	return t
}

func (t *sqlTxn) Commit() (*metaclient.TxnResponse, metaclient.Error) {
	t.mu.Lock()
	if t.Err != nil {
		t.mu.Unlock()
		return nil, t.Err
	}
	if t.committed {
		t.Err = &sqlError{
			displayed: cerrors.ErrMetaCommittedTxn.GenWithStackByArgs("txn had been committed"),
		}
		t.mu.Unlock()
		return nil, t.Err
	}
	t.committed = true
	t.mu.Unlock()

	var txnRsp metaclient.TxnResponse
	txnRsp.Responses = make([]metaclient.ResponseOp, 0, len(t.ops))
	err := t.impl.db.Transaction(func(tx *gorm.DB) error {
		for _, op := range t.ops {
			switch {
			case op.IsGet():
				rsp, err := t.impl.doGet(t.ctx, tx, &op)
				if err != nil {
					return err // rollback
				}
				txnRsp.Responses = append(txnRsp.Responses, makeGetResponseOp(rsp))
			case op.IsPut():
				rsp, err := t.impl.doPut(t.ctx, tx, &op)
				if err != nil {
					return err
				}
				txnRsp.Responses = append(txnRsp.Responses, makePutResponseOp(rsp))
			case op.IsDelete():
				rsp, err := t.impl.doDelete(t.ctx, tx, &op)
				if err != nil {
					return err
				}
				txnRsp.Responses = append(txnRsp.Responses, makeDelResponseOp(rsp))
			case op.IsTxn():
				return &sqlError{
					displayed: cerrors.ErrMetaNestedTxn.GenWithStackByArgs("unsupported nested txn"),
				}
			default:
				return &sqlError{
					displayed: cerrors.ErrMetaOpFail.GenWithStackByArgs("unknown op type"),
				}
			}
		}

		return nil // commit
	})

	if err != nil {
		err2, ok := err.(*sqlError)
		if ok {
			return nil, err2
		}

		return nil, sqlErrorFromOpFail(err2)
	}

	return &txnRsp, nil
}
