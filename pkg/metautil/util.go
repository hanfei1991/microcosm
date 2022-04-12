package metautil

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
)

// TODO: retry and idempotent??
// TODO: context control??

const (
	DefaultConnMaxIdleTime = time.Duration(30)
	DefaultConnMaxLifeTime = time.Duration(18600)
	DefaultMaxIdleConns    = 3
	DefaultMaxOpenConns    = 10
)

type TimeRange struct {
	start time.Time
	end   time.Time
}

// refer to: https://pkg.go.dev/database/sql#SetConnMaxIdleTime
type DBConfig struct {
	ConnMaxIdleTime time.Duration
	ConnMaxLifeTime time.Duration
	MaxIdleConns    int
	MaxOpenConns    int
}

// NewSqlDB return sql.DB for specified driver and dsn
// dsn format: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
// TODO: using projectID as dbname to construct dsn to achieve isolation
func NewSQLDB(driver, dsn string, conf DBConfig) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, cerrors.ErrMetaNewClientFail.GenWithStackByArgs(err)
	}

	db.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
	db.SetConnMaxLifetime(conf.ConnMaxLifeTime)
	db.SetMaxIdleConns(conf.MaxIdleConns)
	db.SetMaxOpenConns(conf.MaxOpenConns)

	return db, nil
}

// NewMetaOpsClient return the client to operate framework metastore
func NewMetaOpsClient(sqlDB *sql.DB) (*MetaOpsClient, error) {
	if sqlDB == nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("para is nil"))
	}

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: sqlDB,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		return nil, err
	}

	return &MetaOpsClient{
		db: db,
	}, nil
}

// MetaOpsClient is the meta operations client for framework metastore
type MetaOpsClient struct {
	// orm related object
	db *gorm.DB
}

////////////////////////// Initialize
// Initialize will create all related tables in SQL backend
// TODO: What if we change the definition of orm??
func (c *MetaOpsClient) Initialize(ctx context.Context) error {
	if err := c.db.AutoMigrate(&ProjectInfo{}, &ProjectOperation{}, &JobInfo{}, &WorkerInfo{}, &ResourceInfo{}); err != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(err)
	}

	return nil
}

///////////////////////// Project Operation
// AddProject insert the ProjectInfo
func (c *MetaOpsClient) AddProject(ctx context.Context, project *ProjectInfo) error {
	if project == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("para is nil"))
	}
	if result := c.db.Create(project); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteProject delete the ProjectInfo
func (c *MetaOpsClient) DeleteProject(ctx context.Context, projectID string) error {
	if result := c.db.Where("project_id=?", projectID).Delete(&ProjectInfo{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// QueryProject query all projects
func (c *MetaOpsClient) QueryProjects(ctx context.Context) ([]ProjectInfo, error) {
	var projects []ProjectInfo
	if result := c.db.Find(&projects); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return projects, nil
}

// GetProjectByID query project by projectID
func (c *MetaOpsClient) GetProjectByID(ctx context.Context, projectID string) (*ProjectInfo, error) {
	var project ProjectInfo
	if result := c.db.Where("project_id = ?", projectID).First(&project); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &project, nil
}

// AddProjectOperation insert the operation
func (c *MetaOpsClient) AddProjectOperation(ctx context.Context, op *ProjectOperation) error {
	if op == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("para is nil"))
	}

	if result := c.db.Create(op); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// QueryProjectOperations query all operations of the projectID
func (c *MetaOpsClient) QueryProjectOperations(ctx context.Context, projectID string) ([]ProjectOperation, error) {
	var projectOps []ProjectOperation
	if result := c.db.Where("project_id = ?", projectID).Find(&projectOps); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return projectOps, nil
}

// QueryProjectOperationsByTimeRange query project operation betweem a time range of the projectID
func (c *MetaOpsClient) QueryProjectOperationsByTimeRange(ctx context.Context,
	projectID string, tr TimeRange,
) ([]ProjectOperation, error) {
	var projectOps []ProjectOperation
	if result := c.db.Where("project_id = ? AND created_at >= ? AND created_at <= ?", projectID, tr.start,
		tr.end).Find(&projectOps); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return projectOps, nil
}

/////////////////////////////// Job Operation
// SetJob insert the jobInfo
func (c *MetaOpsClient) AddJob(ctx context.Context, job *JobInfo) error {
	if job == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("para is nil"))
	}

	if result := c.db.Create(job); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// UpdateJob update the jobInfo
func (c *MetaOpsClient) UpdateJob(ctx context.Context, job *JobInfo) error {
	// TODO: shall we change the model name?
	if result := c.db.Save(job); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteJob delete the specified jobInfo
func (c *MetaOpsClient) DeleteJob(ctx context.Context, jobID string) error {
	if result := c.db.Where("job_id = ?", jobID).Delete(&JobInfo{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// QueryJobByID query job by `jobID`
func (c *MetaOpsClient) GetJobByID(ctx context.Context, jobID string) (*JobInfo, error) {
	var job JobInfo
	if result := c.db.Where("job_id = ?", jobID).First(&job); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &job, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *MetaOpsClient) QueryJobsByProjectID(ctx context.Context, projectID string) ([]JobInfo, error) {
	var jobs []JobInfo
	if result := c.db.Where("project_id = ?", projectID).Find(&jobs); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return jobs, nil
}

// QueryJobsByStatus query all jobs with `status` of the projectID
func (c *MetaOpsClient) QueryJobsByStatus(ctx context.Context,
	jobID string, status int,
) ([]JobInfo, error) {
	var jobs []JobInfo
	if result := c.db.Where("job_id = ? AND job_status = ?", jobID, status).Find(&jobs); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return jobs, nil
}

/////////////////////////////// Worker Operation
// AddWorker insert the workerInfo
func (c *MetaOpsClient) AddWorker(ctx context.Context, worker *WorkerInfo) error {
	if worker == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("para is nil"))
	}

	if result := c.db.Create(worker); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

func (c *MetaOpsClient) UpdateWorker(ctx context.Context, worker *WorkerInfo) error {
	// TODO: shall we change the model name?
	if result := c.db.Save(worker); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteWorker delete the specified workInfo
func (c *MetaOpsClient) DeleteWorker(ctx context.Context, masterID string, workerID string) error {
	if result := c.db.Where("job_id = ? AND worker_id = ?", masterID,
		workerID).Delete(&WorkerInfo{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// GetWorkerByID query worker info by workerID
func (c *MetaOpsClient) GetWorkerByID(ctx context.Context, masterID string, workerID string) (*WorkerInfo, error) {
	var worker WorkerInfo
	if result := c.db.Where("job_id = ? AND worker_id = ?", masterID,
		workerID).First(&worker); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &worker, nil
}

// QueryWorkersByMasterID query all workers of masterID
func (c *MetaOpsClient) QueryWorkersByMasterID(ctx context.Context, masterID string) ([]WorkerInfo, error) {
	var workers []WorkerInfo
	if result := c.db.Where("job_id = ?", masterID).Find(&workers); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return workers, nil
}

// QueryWorkersByStatus query all workers with specified status of masterID
func (c *MetaOpsClient) QueryWorkersByStatus(ctx context.Context, masterID string, status int) ([]WorkerInfo, error) {
	var workers []WorkerInfo
	if result := c.db.Where("job_id = ? AND worker_status = ?", masterID,
		status).Find(&workers); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return workers, nil
}

/////////////////////////////// Resource Operation
// AddResource insert the ResourceInfo
func (c *MetaOpsClient) AddResource(ctx context.Context, resource *ResourceInfo) error {
	if resource == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("para is nil"))
	}

	if result := c.db.Create(resource); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

func (c *MetaOpsClient) UpdateResource(ctx context.Context, resource *ResourceInfo) error {
	// TODO: shall we change the model name?
	if result := c.db.Save(resource); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteResource delete the specified ResourceInfo
func (c *MetaOpsClient) DeleteResource(ctx context.Context, resourceID string) error {
	if result := c.db.Where("resource_id = ?", resourceID).Delete(&ResourceInfo{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// QueryResourceByID query resource of the resource_id
func (c *MetaOpsClient) GetResourceByID(ctx context.Context, resourceID string) (*ResourceInfo, error) {
	var resource ResourceInfo
	if result := c.db.Where("resource_id = ?", resourceID).
		First(&resource); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &resource, nil
}

// QueryResourcesByJobID query all resources of the jobID
func (c *MetaOpsClient) QueryResourcesByJobID(ctx context.Context, jobID string) ([]ResourceInfo, error) {
	var resources []ResourceInfo
	if result := c.db.Where("job_id = ?", jobID).Find(&resources); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return resources, nil
}

// QueryResourcesByExecutorID query all resources of the executor_id
func (c *MetaOpsClient) QueryResourcesByExecutorID(ctx context.Context, executorID string) ([]ResourceInfo, error) {
	var resources []ResourceInfo
	if result := c.db.Where("executor_id = ?", executorID).Find(&resources); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return resources, nil
}
