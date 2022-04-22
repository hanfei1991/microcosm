package metadata

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	derror "github.com/hanfei1991/microcosm/pkg/errors"
	dorm "github.com/hanfei1991/microcosm/pkg/meta/orm"
	libModel "github.com/hanfei1991/microcosm/pkg/meta/orm/model"
)

const JobManagerUUID = "dataflow-engine-job-manager"

type MasterMetadataClient struct {
	projectID  libModel.ProjectID
	masterID   libModel.MasterID
	metaClient *dorm.MetaOpsClient
}

func NewMasterMetadataClient(
	projectID libModel.ProjectID,
	masterID libModel.MasterID,
	metaClient *dorm.MetaOpsClient) *MasterMetadataClient {
	return &MasterMetadataClient{
		projectID:  projectID,
		masterID:   masterID,
		metaClient: metaClient,
	}
}

func (c *MasterMetadataClient) Load(ctx context.Context) (*libModel.MasterMeta, error) {
	// TODO: what if don't exists
	// NotfoundErr
	masterMeta, err := c.metaclient.GetJobByID(ctx, c.masterID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return masterMeta, nil
}

func (c *MasterMetadataClient) Store(ctx context.Context, data *libModel.MasterMeta) error {
	// TODO: Add? Update? data has ID?
	err := c.metaClient.UpdateJob(ctx, data)
	return errors.Trace(err)
}

func (c *MasterMetadataClient) Delete(ctx context.Context) error {
	err := c.metaClient.DeleteJob(ctx, c.masterID)
	return errors.Trace(err)
}

// LoadAllMasters loads all job masters from metastore
func (c *MasterMetadataClient) LoadAllMasters(ctx context.Context) ([]*libModel.MasterMeta, error) {
	resp, err := c.metaClient.QueryJobsByProjectID(ctx, c.projectID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

type WorkerMetadataClient struct {
	projectID  libModel.ProjectID
	masterID   libModel.MasterID
	metaClient metaclient.MetaOpsClient
}

func NewWorkerMetadataClient(
	projectID libModel.ProjectID,
	masterID libModel.MasterID,
	metaClient *dorm.MetaOpsClient,
) *WorkerMetadataClient {
	return &WorkerMetadataClient{
		projectID:  projectID,
		masterID:   masterID,
		metaClient: metaClient,
	}
}

func (c *WorkerMetadataClient) LoadAllWorkers(ctx context.Context) (map[libModel.WorkerID]*libModel.WorkerMeta, error) {
	resp, err := c.metaClient.QueryWorkersByMasterID(ctx, c.masterID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	res := make(map[libModel.WorkerID]*libModel.WorkerMeta, len(resp))
	for _, m := range resp {
		res[m.ID] = m
	}
	return res, nil
}

func (c *WorkerMetadataClient) Load(ctx context.Context, workerID libModel.WorkerID) (*libModel.WorkerMeta, error) {
	resp, err := c.metaClient.GetWorkerByID(ctx, c.masterID, workerID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return resp, nil
}

func (c *WorkerMetadataClient) Remove(ctx context.Context, id libModel.WorkerID) (bool, error) {
	err := c.metaClient.DeleteWorker(ctx, c.mastersID, id)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (c *WorkerMetadataClient) Store(ctx context.Context, workerID libModel.WorkerID, data *libModel.WorkerMeta) error {
	// TODO add? Update?
	err = c.metaClient.UpdateWorker(ctx, data)
	return errors.Trace(err)
}

func (c *WorkerMetadataClient) MasterID() libModel.MasterID {
	return c.masterID
}

// StoreMasterMeta is exposed to job manager for job master meta persistence
func StoreMasterMeta(
	ctx context.Context,
	metaClient *dorm.MetaOpsClient,
	meta *libModel.MasterMeta,
) error {
	metaClient := NewMasterMetadataClient(meta.ProjectID, meta.ID, metaClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		if !derror.ErrMasterNotFound.Equal(err) {
			return err
		}
	} else {
		log.L().Warn("master meta exits, will be overwritten", zap.Any("old-meta", masterMeta), zap.Any("meta", meta))
	}

	return metaClient.Store(ctx, meta)
}

func DeleteMasterMeta(
	ctx context.Context,
	metaClient *dorm.MetaOpsClient,
	masterID libModel.MasterID,
) error {
	metaClient := NewMasterMetadataClient("", masterID, metaClient)
	return metaClient.Delete(ctx)
}
