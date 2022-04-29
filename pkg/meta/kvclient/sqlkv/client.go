package sqlkv

import (
	"database/sql"

	ormModel "github.com/hanfei1991/microcosm/pkg/orm/model"
	"github.com/hanfei1991/microcosm/pkg/sqlutil"
	"gorm.io/gorm"
)

// sqlImpl is the mysql-compatiable implement for KVClient
type sqlImpl struct {
	// gorm claim to be thread safe
	db   *gorm.DB
	impl *sql.DB
}

func NewSQLImpl(config *metaclient.StoreConfigParams, projectID tenant.ProjectID,
	sqlConf sqlutil.DBConfig) (*sqlImpl, error) {
	err := sqlutil.CreateDatabaseForProject(mc, projectID, conf)
	if err != nil {
		return nil, err
	}

	dsn := sqlutil.GenerateDSNByParams(mc, projectID, conf, true)
	sqlDB, err := sqlutil.NewSQLDB("mysql", dsn, conf)
	if err != nil {
		return nil, err
	}

	cli, err := newClient(sqlDB)
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

func (s *sqlImpl) Close() {
	if s.impl != nil {
		return s.impl.Close()
	}

	return nil
}

// Initialize will create all related tables in SQL backend
// TODO: What if we change the definition of orm??
func (s *sqlImpl) Initialize(ctx context.Context) error {
	if err := c.db.AutoMigrate(&KeyValue{}, &ormModel.LogicEpoch{}); err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	// check first record in logic_epochs
	return c.InitializeEpoch(ctx)
}

/////////////////////////////// Logic Epoch
// TODO: what if the record is deleted manually??
func (s *sqlImpl) InitializeEpoch(ctx context.Context) error {
	var logicEp model.LogicEpoch
	// first check and add first record if not exists
	if result := c.db.First(&logicEp, defaultEpochPK); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			if res := c.db.Create(&model.LogicEpoch{
				Model: model.Model{
					SeqID: defaultEpochPK,
				},
				Epoch: defaultMinEpoch,
			}); res.Error != nil {
				return cerrors.ErrMetaOpFail.Wrap(res.Error)
			}

			return nil
		}

		return cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	// already exists, do nothing
	return nil
}

func (s *sqlImpl) GenEpoch(ctx context.Context) (int64, error) {
	var epoch int64
	err := c.db.Transaction(func(tx *gorm.DB) error {
		//(1)update epoch = epoch + 1
		if err := tx.Model(&model.LogicEpoch{
			Model: model.Model{
				SeqID: defaultEpochPK,
			},
		}).Update("epoch", gorm.Expr("epoch + ?", 1)).Error; err != nil {
			// return any error will rollback
			return err
		}

		//(2)select epoch
		var logicEp model.LogicEpoch
		if err := tx.First(&logicEp, defaultEpochPK).Error; err != nil {
			return err
		}
		epoch = libModel.Epoch(logicEp.Epoch)

		// return nil will commit the whole transaction
		return nil
	})
	if err != nil {
		return 0, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return epoch, nil
}

func (c *sqlImpl) Put(ctx context.Context, key, val string) (*metaclient.PutResponse, metaclient.Error) {
	sql := "REPLACE INTO `key_values`(`key`, `value`) VALUES(?, ?)"
	if err := s.db.Exec(sql, key, value).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{
			//TODO: clusterID
		},
	}, nil
}

func (c *sqlImpl) Get(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.GetResponse, metaclient.Error) {
	op := metaclient.OpGet(key, opts...)
	if err := op.CheckValidOp(); err != nil {
		return nil, sqlErrorFromOpFail(err)
	}

	var kvs []KeyValue 
	var err error
	if op.IsOptsWithRange() {
		err = c.db.Where("key >= ? && key < ?").Find(&kvs)	
	}else if op.IsOptsWithPrefix() {
		err = c.db.Where("key like ")
	}



	if err := c.db.Where("key = ?", key).First(&pair).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}


	return , nil
}

func (c *sqlImpl) Delete(ctx context.Context, key string, opts ...metaclient.OpOption) (*metaclient.DeleteResponse, metaclient.Error) {
}

func (c *sqlImpl) Do(ctx context.Context, op metaclient.Op) (metaclient.OpResponse, metaclient.Error) {
}

func (c *sqlImpl) Txn(ctx context.Context) metaclient.Txn {
}

func (t *etcdTxn) Do(ops ...metaclient.Op) metaclient.Txn {
}

func (t *etcdTxn) Commit() (*metaclient.TxnResponse, metaclient.Error) {

}

