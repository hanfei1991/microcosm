package metautil

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/stretchr/testify/require"
)

type tCase struct {
	fn              string
	inputs          []interface{}
	output          interface{}
	err             error
	mockExpectResFn func(mock sqlmock.Sqlmock)
}

func mockGetDBConn(t *testing.T, dsnStr string) (*sql.DB, sqlmock.Sqlmock, error) {
	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	// common execution for orm
	mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows(
		[]string{"VERSION()"}).AddRow("5.7.35-log"))
	return db, mock, nil
}

func TestNewMetaOpsClient(t *testing.T) {
	t.Parallel()

	cli, err := NewMetaOpsClient(nil)
	require.Nil(t, cli)
	require.Error(t, err)

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err = NewMetaOpsClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)
}

// nolint: deadcode
func testInitialize(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := NewMetaOpsClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	testCases := []tCase{
		{
			fn:     "Initialize",
			inputs: []interface{}{},
			// TODO: Why index sequence is not stable ??
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE TABLE `project_infos` [(]`id` bigint unsigned AUTO_INCREMENT," +
					"`created_at` datetime[(]3[)] NULL,`updated_at` datetime[(]3[)] NULL," +
					"`project_id` char[(]64[)] not null,`project_name` varchar[(]64[)] not null,PRIMARY KEY [(]`id`[)]," +
					"UNIQUE INDEX uidx_id [(]`project_id`[))]").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("CREATE TABLE `project_operations` [(]`id` bigint unsigned AUTO_INCREMENT," +
					"`project_id` char[(]64[)] not null,`operation` varchar[(]16[)] not null,`job_id` char[(]64[)] not null," +
					"`created_at` datetime[(]3[)] NULL,PRIMARY KEY [(]`id`[)],INDEX idx_op [(]`project_id`,`created_at`[))]").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("CREATE TABLE `job_infos` [(]`id` bigint unsigned AUTO_INCREMENT,`created_at` datetime[(]3[)] NULL," +
					"`updated_at` datetime[(]3[)] NULL,`project_id` char[(]64[)] not null," +
					"`job_id` char[(]64[)] not null,`job_type` tinyint not null,`job_status` tinyint not null," +
					"`job_addr` char[(]64[)] not null,`job_config` longblob,PRIMARY KEY [(]`id`[)]," +
					"UNIQUE INDEX uidx_id [(]`job_id`[)],INDEX idx_st [(]`project_id`,`job_status`[))]").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("CREATE TABLE `worker_infos` [(]`id` bigint unsigned AUTO_INCREMENT," +
					"`created_at` datetime[(]3[)] NULL,`updated_at` datetime[(]3[)] NULL," +
					"`project_id` char[(]64[)] not null,`job_id` char[(]64[)] not null,`worker_id` char[(]64[)] not null," +
					"`worker_type` tinyint not null,`worker_status` tinyint not null,`worker_err_msg` varchar[(]128[)]," +
					"`worker_config` blob,PRIMARY KEY [(]`id`[)],UNIQUE INDEX uidx_id [(]`job_id`,`worker_id`[)]," +
					"INDEX idx_st [(]`job_id`,`worker_status`[))]").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("CREATE TABLE `resource_infos` [(]`id` bigint unsigned AUTO_INCREMENT,`created_at` datetime[(]3[)] NULL," +
					"`updated_at` datetime[(]3[)] NULL,`project_id` char[(]64[)] not null," +
					"`resource_id` char[(]64[)] not null,`job_id` char[(]64[)] not null,`worker_id` char[(]64[)] not null," +
					"`executor_id` char[(]64[)] not null,`resource_status` BOOLEAN,PRIMARY KEY [(]`id`[)]," +
					"UNIQUE INDEX uidx_id [(]`resource_id`[)]," +
					"INDEX idx_ji [(]`job_id`,`resource_id`[)],INDEX idx_ei [(]`executor_id`,`resource_id`[))]").WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestProject(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := NewMetaOpsClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			fn: "AddProject",
			inputs: []interface{}{
				&ProjectInfo{
					Model: Model{
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:   "p111",
					ProjectName: "tenant1",
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `project_infos` [(]`created_at`,`updated_at`,`project_id`,"+
					"`project_name`[)]").WithArgs(createdAt, updatedAt, "p111", "tenant1").WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "AddProject",
			inputs: []interface{}{
				&ProjectInfo{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:   "p111",
					ProjectName: "tenant2",
				},
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `project_infos` [(]`created_at`,`updated_at`,`project_id`,"+
					"`project_name`,`id`[)]").WithArgs(createdAt, updatedAt, "p111", "tenant2", 1).WillReturnError(errors.New("projectID is duplicated"))
			},
		},
		{
			fn: "DeleteProject",
			inputs: []interface{}{
				"p111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `project_infos` WHERE project_id").WithArgs("p111").WillReturnError(errors.New("DeleteProject error"))
			},
		},
		{
			fn: "DeleteProject",
			inputs: []interface{}{
				"p111",
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `project_infos` WHERE project_id").WithArgs("p111").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn:     "QueryProjects",
			inputs: []interface{}{},
			output: []ProjectInfo{
				{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:   "p111",
					ProjectName: "tenant1",
				},
				{
					Model: Model{
						ID:        2,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:   "p111",
					ProjectName: "tenant2",
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos`").WillReturnRows(sqlmock.NewRows([]string{
					"created_at", "updated_at", "project_id", "project_name",
					"id",
				}).AddRow(createdAt, updatedAt, "p111", "tenant1", 1).AddRow(createdAt, updatedAt, "p111", "tenant2", 2))
			},
		},
		{
			fn:     "QueryProjects",
			inputs: []interface{}{},
			err:    cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos`").WillReturnError(errors.New("QueryProjects error"))
			},
		},
		{
			// SELECT * FROM `project_infos` WHERE project_id = '111-222-333' ORDER BY `project_infos`.`id` LIMIT 1
			fn: "GetProjectByID",
			inputs: []interface{}{
				"111-222-333",
			},
			output: &ProjectInfo{
				Model: Model{
					ID:        2,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ProjectID:   "p111",
				ProjectName: "tenant1",
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos` WHERE project_id").WithArgs("111-222-333").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "project_name",
						"id",
					}).AddRow(createdAt, updatedAt, "p111", "tenant1", 2))
			},
		},
		{
			fn: "GetProjectByID",
			inputs: []interface{}{
				"p111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_infos` WHERE project_id").WithArgs("p111").WillReturnError(
					errors.New("GetProjectByID error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestProjectOperation(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := NewMetaOpsClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	tm1 := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			// SELECT * FROM `project_operations` WHERE project_id = '111'
			fn: "QueryProjectOperations",
			inputs: []interface{}{
				"p111",
			},
			output: []ProjectOperation{
				{
					ID:        1,
					ProjectID: "p111",
					Operation: "Submit",
					JobID:     "j222",
					CreatedAt: tm,
				},
				{
					ID:        2,
					ProjectID: "p112",
					Operation: "Drop",
					JobID:     "j222",
					CreatedAt: tm1,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111").WillReturnRows(
					sqlmock.NewRows([]string{"id", "project_id", "operation", "job_id", "created_at"}).AddRow(
						1, "p111", "Submit", "j222", tm).AddRow(
						2, "p112", "Drop", "j222", tm1))
			},
		},
		{
			fn: "QueryProjectOperations",
			inputs: []interface{}{
				"p111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111").WillReturnError(errors.New("QueryProjectOperations error"))
			},
		},
		{
			// SELECT * FROM `project_operations` WHERE project_id = '111' AND created_at >= '2022-04-13 23:51:42.46' AND created_at <= '2022-04-13 23:51:42.46'
			fn: "QueryProjectOperationsByTimeRange",
			inputs: []interface{}{
				"p111",
				TimeRange{
					start: tm,
					end:   tm1,
				},
			},
			output: []ProjectOperation{
				{
					ID:        1,
					ProjectID: "p111",
					Operation: "Submit",
					JobID:     "j222",
					CreatedAt: tm,
				},
				{
					ID:        2,
					ProjectID: "p112",
					Operation: "Drop",
					JobID:     "j222",
					CreatedAt: tm1,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111", tm, tm1).WillReturnRows(
					sqlmock.NewRows([]string{"id", "project_id", "operation", "job_id", "created_at"}).AddRow(
						1, "p111", "Submit", "j222", tm).AddRow(
						2, "p112", "Drop", "j222", tm1))
			},
		},
		{
			fn: "QueryProjectOperationsByTimeRange",
			inputs: []interface{}{
				"p111",
				TimeRange{
					start: tm,
					end:   tm1,
				},
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `project_operations` WHERE project_id").WithArgs("p111", tm, tm1).WillReturnError(
					errors.New("QueryProjectOperationsByTimeRange error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestJob(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := NewMetaOpsClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			fn: "AddJob",
			inputs: []interface{}{
				&JobInfo{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					JobID:     "j111",
					JobType:   1,
					JobStatus: 1,
					JobAddr:   "127.0.0.1",
					JobConfig: []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `job_infos` [(]`created_at`,`updated_at`,`project_id`,`job_id`,"+
					"`job_type`,`job_status`,`job_addr`,`job_config`,`id`[)]").WithArgs(createdAt, updatedAt, "p111",
					"j111", 1, 1, "127.0.0.1", []byte{0x11, 0x22}, 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			// INSERT INTO `job_infos` (`created_at`,`updated_at`,`project_id`,`job_id`,`job_type`,`job_status`,`job_addr`,
			// `job_config`,`id`) VALUES ('2022-04-14 10:56:50.557','2022-04-14 10:56:50.557','111-222-333','111',1,1,'127.0.0.1','<binary>',1)
			fn: "AddJob",
			inputs: []interface{}{
				&JobInfo{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					JobID:     "j111",
					JobType:   1,
					JobStatus: 1,
					JobAddr:   "127.0.0.1",
					JobConfig: []byte{0x11, 0x22},
				},
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `job_infos` [(]`created_at`,`updated_at`,`project_id`,`job_id`,"+
					"`job_type`,`job_status`,`job_addr`,`job_config`,`id`[)]").WithArgs(createdAt, updatedAt, "p111",
					"j111", 1, 1, "127.0.0.1", []byte{0x11, 0x22}, 1).WillReturnError(errors.New("AddJob error"))
			},
		},
		{
			fn: "DeleteJob",
			inputs: []interface{}{
				"j111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `job_infos` WHERE job_id").WithArgs(
					"j111").WillReturnError(errors.New("DeleteJob error"))
			},
		},
		{
			// DELETE FROM `job_infos` WHERE project_id = '111-222-334' AND job_id = '111'
			fn: "DeleteJob",
			inputs: []interface{}{
				"j112",
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `job_infos` WHERE job_id").WithArgs(
					"j112").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// SELECT * FROM `job_infos` WHERE project_id = '111-222-333' AND job_id = '111' ORDER BY `job_infos`.`id` LIMIT 1
			fn: "GetJobByID",
			inputs: []interface{}{
				"j111",
			},
			output: &JobInfo{
				Model: Model{
					ID:        1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ProjectID: "p111",
				JobID:     "j111",
				JobType:   1,
				JobStatus: 1,
				JobAddr:   "127.0.0.1",
				JobConfig: []byte{0x11, 0x22},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `job_infos` WHERE job_id").WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"job_type", "job_status", "job_addr", "job_config", "id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "127.0.0.1", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "GetJobByID",
			inputs: []interface{}{
				"j111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `job_infos` WHERE job_id").WithArgs("j111").WillReturnError(
					errors.New("GetJobByID error"))
			},
		},
		{
			// SELECT * FROM `job_infos` WHERE project_id = '111-222-333'
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p111",
			},
			output: []JobInfo{
				{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					JobID:     "j111",
					JobType:   1,
					JobStatus: 1,
					JobAddr:   "1.1.1.1",
					JobConfig: []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `job_infos` WHERE project_id").WithArgs("p111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"job_type", "job_status", "job_addr", "job_config", "id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "1.1.1.1", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryJobsByProjectID",
			inputs: []interface{}{
				"p111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `job_infos` WHERE project_id").WithArgs("p111").WillReturnError(
					errors.New("QueryJobsByProjectID error"))
			},
		},
		{
			//  SELECT * FROM `job_infos` WHERE project_id = '111-222-333' AND job_status = 1
			fn: "QueryJobsByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			output: []JobInfo{
				{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID: "p111",
					JobID:     "j111",
					JobType:   1,
					JobStatus: 1,
					JobAddr:   "127.0.0.1",
					JobConfig: []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `job_infos` WHERE job_id").WithArgs("j111", 1).WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"job_type", "job_status", "job_addr", "job_config", "id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", 1, 1, "127.0.0.1", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryJobsByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `job_infos` WHERE job_id").WithArgs("j111", 1).WillReturnError(
					errors.New("QueryJobsByStatus error"))
			},
		},
		// UpdatedAt will be updated inside the orm lib, which is hard to get the oriented time.Time to set expectation
		/*
			{
				// 'UPDATE `job_infos` SET `created_at`=?,`updated_at`=?,`project_id`=?,
				// `job_id`=?,`job_type`=?,`job_status`=?,`job_addr`=?,`job_config`=? WHERE `id` = ?'
				fn: "UpdateJob",
				inputs: []interface{}{
					&JobInfo{
						Model: Model{
							ID:        1,
							CreatedAt: createdAt,
							UpdatedAt: updatedAt,
						},
						ProjectID: "p111",
						JobID:     "j111",
						JobType:   1,
						JobStatus: 1,
						JobAddr:   "127.0.0.1",
						JobConfig: []byte{0x11, 0x22},
					},
				},
				mockExpectResFn: func(mock sqlmock.Sqlmock) {
					mock.ExpectExec("UPDATE `job_infos` SET").WithArgs(createdAt, updatedAt, "p111", "j111", 1, 1, "127.0.0.1",
						[]byte{0x11, 0x22}, 1).WillReturnResult(sqlmock.NewResult(1, 1))
				},
			},
		*/
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestWorker(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := NewMetaOpsClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			// INSERT INTO `worker_infos` (`created_at`,`updated_at`,`project_id`,`job_id`,`worker_id`,`worker_type`,
			// `worker_status`,`worker_err_msg`,`worker_config`,`id`) VALUES ('2022-04-14 11:35:06.119','2022-04-14 11:35:06.119',
			// '111-222-333','111','222',1,1,'error','<binary>',1)
			fn: "AddWorker",
			inputs: []interface{}{
				&WorkerInfo{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					WorkerID:     "w222",
					WorkerType:   1,
					WorkerStatus: 1,
					WorkerErrMsg: "error",
					WorkerConfig: []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `worker_infos` [(]`created_at`,`updated_at`,`project_id`,`job_id`,"+
					"`worker_id`,`worker_type`,`worker_status`,`worker_err_msg`,`worker_config`,`id`[)]").WithArgs(
					createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "AddWorker",
			inputs: []interface{}{
				&WorkerInfo{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					WorkerID:     "w222",
					WorkerType:   1,
					WorkerStatus: 1,
					WorkerErrMsg: "error",
					WorkerConfig: []byte{0x11, 0x22},
				},
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `worker_infos` [(]`created_at`,`updated_at`,`project_id`,`job_id`,"+
					"`worker_id`,`worker_type`,`worker_status`,`worker_err_msg`,`worker_config`,`id`[)]").WithArgs(
					createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1).WillReturnError(errors.New("AddWorker error"))
			},
		},
		{
			fn: "DeleteWorker",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `worker_infos` WHERE job_id").WithArgs(
					"j111", "w222").WillReturnError(errors.New("DeleteWorker error"))
			},
		},
		{
			// DELETE FROM `worker_infos` WHERE project_id = '111-222-334' AND job_id = '111' AND worker_id = '222'
			fn: "DeleteWorker",
			inputs: []interface{}{
				"j112",
				"w223",
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `worker_infos` WHERE job_id").WithArgs(
					"j112", "w223").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			// SELECT * FROM `worker_infos` WHERE project_id = '111-222-333' AND job_id = '111' AND
			// worker_id = '222' ORDER BY `worker_infos`.`id` LIMIT 1
			fn: "GetWorkerByID",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			output: &WorkerInfo{
				Model: Model{
					ID:        1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ProjectID:    "p111",
				JobID:        "j111",
				WorkerID:     "w222",
				WorkerType:   1,
				WorkerStatus: 1,
				WorkerErrMsg: "error",
				WorkerConfig: []byte{0x11, 0x22},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_infos` WHERE job_id").WithArgs("j111", "w222").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"worker_id", "worker_type", "worker_status", "worker_err_msg", "worker_config", "id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "GetWorkerByID",
			inputs: []interface{}{
				"j111",
				"w222",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_infos` WHERE job_id").WithArgs("j111", "w222").WillReturnError(
					errors.New("GetWorkerByID error"))
			},
		},
		{
			// SELECT * FROM `worker_infos` WHERE project_id = '111-222-333' AND job_id = '111'
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j111",
			},
			output: []WorkerInfo{
				{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					WorkerID:     "w222",
					WorkerType:   1,
					WorkerStatus: 1,
					WorkerErrMsg: "error",
					WorkerConfig: []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_infos` WHERE job_id").WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"worker_id", "worker_type", "worker_status", "worker_err_msg", "worker_config", "id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryWorkersByMasterID",
			inputs: []interface{}{
				"j111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_infos` WHERE job_id").WithArgs("j111").WillReturnError(
					errors.New("QueryWorkersByMasterID error"))
			},
		},
		{
			// SELECT * FROM `worker_infos` WHERE project_id = '111-222-333' AND job_id = '111' AND worker_status = 1
			fn: "QueryWorkersByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			output: []WorkerInfo{
				{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ProjectID:    "p111",
					JobID:        "j111",
					WorkerID:     "w222",
					WorkerType:   1,
					WorkerStatus: 1,
					WorkerErrMsg: "error",
					WorkerConfig: []byte{0x11, 0x22},
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_infos` WHERE job_id").WithArgs("j111", 1).WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "job_id",
						"worker_id", "worker_type", "worker_status", "worker_err_msg", "worker_config", "id",
					}).AddRow(
						createdAt, updatedAt, "p111", "j111", "w222", 1, 1, "error", []byte{0x11, 0x22}, 1))
			},
		},
		{
			fn: "QueryWorkersByStatus",
			inputs: []interface{}{
				"j111",
				1,
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `worker_infos` WHERE job_id").WithArgs("j111", 1).WillReturnError(
					errors.New("QueryWorkersByStatus error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func TestResource(t *testing.T) {
	t.Parallel()

	sqlDB, mock, err := mockGetDBConn(t, "test")
	defer sqlDB.Close()
	defer mock.ExpectClose()
	require.Nil(t, err)
	cli, err := NewMetaOpsClient(sqlDB)
	require.Nil(t, err)
	require.NotNil(t, cli)

	tm := time.Now()
	createdAt := tm.Add(time.Duration(1))
	updatedAt := tm.Add(time.Duration(1))

	testCases := []tCase{
		{
			// INSERT INTO `resource_infos` (`created_at`,`updated_at`,`project_id`,`job_id`,
			// `resource_id`,`worker_id`,`executor_id`,`resource_status`,`id`) VALUES ('2022-04-14 12:16:53.353',
			// '2022-04-14 12:16:53.353','111-222-333','j111','r333','w222','e444',true,1)
			fn: "AddResource",
			inputs: []interface{}{
				&ResourceInfo{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ResourceID:     "r333",
					ProjectID:      "111-222-333",
					JobID:          "j111",
					WorkerID:       "w222",
					ExecutorID:     "e444",
					ResourceStatus: true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `resource_infos` [(]`created_at`,`updated_at`,`project_id`,`resource_id`,`job_id`,"+
					"`worker_id`,`executor_id`,`resource_status`,`id`[)]").WithArgs(
					createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1).WillReturnResult(sqlmock.NewResult(1, 1))
			},
		},
		{
			fn: "AddResource",
			inputs: []interface{}{
				&ResourceInfo{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ResourceID:     "r333",
					ProjectID:      "111-222-333",
					JobID:          "j111",
					WorkerID:       "w222",
					ExecutorID:     "e444",
					ResourceStatus: true,
				},
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `resource_infos` [(]`created_at`,`updated_at`,`project_id`,`resource_id`,`job_id`,"+
					"`worker_id`,`executor_id`,`resource_status`,`id`[)]").WithArgs(
					createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1).WillReturnError(errors.New("AddResource error"))
			},
		},
		{
			fn: "DeleteResource",
			inputs: []interface{}{
				"r222",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `resource_infos` WHERE resource_id").WithArgs(
					"r222").WillReturnError(errors.New("DeleteReource error"))
			},
		},
		{
			fn: "DeleteResource",
			inputs: []interface{}{
				"r223",
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `resource_infos` WHERE resource_id").WithArgs(
					"r223").WillReturnResult(sqlmock.NewResult(0, 1))
			},
		},
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				"r222",
			},
			output: &ResourceInfo{
				Model: Model{
					ID:        1,
					CreatedAt: createdAt,
					UpdatedAt: updatedAt,
				},
				ResourceID:     "r333",
				ProjectID:      "111-222-333",
				JobID:          "j111",
				WorkerID:       "w222",
				ExecutorID:     "e444",
				ResourceStatus: true,
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `resource_infos` WHERE resource_id").WithArgs("r222").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "resource_id", "job_id",
						"worker_id", "executor_id", "resource_status", "id",
					}).AddRow(
						createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "GetResourceByID",
			inputs: []interface{}{
				"r222",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `resource_infos` WHERE resource_id").WithArgs("r222").WillReturnError(
					errors.New("GetResourceByID error"))
			},
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j111",
			},
			output: []ResourceInfo{
				{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ResourceID:     "r333",
					ProjectID:      "111-222-333",
					JobID:          "j111",
					WorkerID:       "w222",
					ExecutorID:     "e444",
					ResourceStatus: true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `resource_infos` WHERE job_id").WithArgs("j111").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "resource_id", "job_id",
						"worker_id", "executor_id", "resource_status", "id",
					}).AddRow(
						createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "QueryResourcesByJobID",
			inputs: []interface{}{
				"j111",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `resource_infos` WHERE job_id").WithArgs("j111").WillReturnError(
					errors.New("QueryResourcesByJobID error"))
			},
		},
		{
			fn: "QueryResourcesByExecutorID",
			inputs: []interface{}{
				"e444",
			},
			output: []ResourceInfo{
				{
					Model: Model{
						ID:        1,
						CreatedAt: createdAt,
						UpdatedAt: updatedAt,
					},
					ResourceID:     "r333",
					ProjectID:      "111-222-333",
					JobID:          "j111",
					WorkerID:       "w222",
					ExecutorID:     "e444",
					ResourceStatus: true,
				},
			},
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `resource_infos` WHERE executor_id").WithArgs("e444").WillReturnRows(
					sqlmock.NewRows([]string{
						"created_at", "updated_at", "project_id", "resource_id", "job_id",
						"worker_id", "executor_id", "resource_status", "id",
					}).AddRow(
						createdAt, updatedAt, "111-222-333", "r333", "j111", "w222", "e444", true, 1))
			},
		},
		{
			fn: "QueryResourcesByExecutorID",
			inputs: []interface{}{
				"e444",
			},
			err: cerrors.ErrMetaOpFail.GenWithStackByArgs(),
			mockExpectResFn: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT [*] FROM `resource_infos` WHERE executor_id").WithArgs("e444").WillReturnError(
					errors.New("QueryResourcesByExecutorID error"))
			},
		},
	}

	for _, tc := range testCases {
		testInner(t, mock, cli, tc)
	}
}

func testInner(t *testing.T, m sqlmock.Sqlmock, cli *MetaOpsClient, c tCase) {
	// set the mock expectation
	c.mockExpectResFn(m)

	var args []reflect.Value
	args = append(args, reflect.ValueOf(context.Background()))
	for _, ip := range c.inputs {
		args = append(args, reflect.ValueOf(ip))
	}
	result := reflect.ValueOf(cli).MethodByName(c.fn).Call(args)
	// only error
	if len(result) == 1 {
		if c.err == nil {
			require.Nil(t, result[0].Interface())
		} else {
			require.NotNil(t, result[0].Interface())
			require.Error(t, result[0].Interface().(error))
		}
	} else if len(result) == 2 {
		// result and error
		if c.err != nil {
			require.NotNil(t, result[1].Interface())
			require.Error(t, result[1].Interface().(error))
		} else {
			require.Equal(t, c.output, result[0].Interface())
		}
	}
}
