package orm

import (
	"context"
	"time"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite" // Sqlite driver based on GGO
	"gorm.io/gorm"
)

// NewMockClient creates a mock orm client
func NewMockClient() (Client, error) {
	db, err := gorm.Open(sqlite.Open("file:test?mode=memory"), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		log.L().Error("create gorm client fail", zap.Error(err))
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	cli := &metaOpsClient{
		db:   db,
		impl: nil, //TODO: need close embeded db
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Initialize(ctx); err != nil {
		cli.Close()
		return nil, err
	}

	return cli, nil
}
