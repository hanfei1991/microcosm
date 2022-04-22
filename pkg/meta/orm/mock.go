package orm

import (
	"github.com/DATA-DOG/go-sqlmock"
	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/log"
)

func NewMockMetaOpsClient() (*MetaOpsClient, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		log.L().Error("create sql mock fail")
		return cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	cli, err := newMetaClient(db)
	if err != nil {
		log.L().Error("create metaops client fail")
		return cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows(
		[]string{"VERSION()"}).AddRow("5.7.35-log"))
	return cli, mock, nil
}
