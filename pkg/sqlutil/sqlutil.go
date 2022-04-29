package sqlutil

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// TODO: check the projectID
func CreateDatabaseForProject(mc metaclient.StoreConfigParams, projectID tenant.ProjectID, conf DBConfig) error {
	dsn := generateDSNByParams(mc, projectID, conf, false)
	log.L().Info("mysql connection", zap.String("dsn", dsn))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.L().Error("open dsn fail", zap.String("dsn", dsn), zap.Error(err))
		return cerrors.ErrMetaOpFail.Wrap(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	query := fmt.Sprintf("CREATE DATABASE if not exists %s", projectID)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// generateDSNByParams will use projectID as DBName to achieve isolation.
// Besides, it will add some default mysql params to the dsn
func GenerateDSNByParams(mc metaclient.StoreConfigParams, projectID tenant.ProjectID,
	conf DBConfig, withDB bool) string {
	dsnCfg := dmysql.NewConfig()
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.User = mc.User
	dsnCfg.Passwd = mc.Password
	dsnCfg.Net = "tcp"
	dsnCfg.Addr = mc.Endpoints[0]
	if withDB {
		dsnCfg.DBName = projectID
	}
	dsnCfg.InterpolateParams = true
	// dsnCfg.MultiStatements = true
	dsnCfg.Params["readTimeout"] = conf.ReadTimeout
	dsnCfg.Params["writeTimeout"] = conf.WriteTimeout
	dsnCfg.Params["timeout"] = conf.DialTimeout
	dsnCfg.Params["parseTime"] = "true"
	// TODO: check for timezone
	dsnCfg.Params["loc"] = "Local"

	// dsn format: [username[:password]@][protocol[(address)]]/
	return dsnCfg.FormatDSN()
}

// newSqlDB return sql.DB for specified driver and dsn
func NewSQLDB(driver string, dsn string, conf DBConfig) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.L().Error("open dsn fail", zap.String("dsn", dsn), zap.Any("config", conf), zap.Error(err))
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	db.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
	db.SetConnMaxLifetime(conf.ConnMaxLifeTime)
	db.SetMaxIdleConns(conf.MaxIdleConns)
	db.SetMaxOpenConns(conf.MaxOpenConns)
	return db, nil
}
