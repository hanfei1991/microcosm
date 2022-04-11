package metadata

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

const JobManagerUUID = "dataflow-engine-job-manager"

type MasterMetadataClient struct {
	masterID     libModel.MasterID
	metaKVClient metaclient.KVClient
}

func NewMasterMetadataClient(masterID libModel.MasterID, metaKVClient metaclient.KVClient) *MasterMetadataClient {
	return &MasterMetadataClient{
		masterID:     masterID,
		metaKVClient: metaKVClient,
	}
}

func (c *MasterMetadataClient) Load(ctx context.Context) (*libModel.MasterMetaKVData, error) {
	key := adapter.MasterMetaKey.Encode(c.masterID)
	resp, err := c.metaKVClient.Get(ctx, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		// TODO refine handling the situation where the mata key does not exist at this point
		masterMeta := &libModel.MasterMetaKVData{
			ID:         c.masterID,
			StatusCode: libModel.MasterStatusUninit,
		}
		return masterMeta, nil
	}
	masterMetaBytes := resp.Kvs[0].Value
	var masterMeta libModel.MasterMetaKVData
	if err := json.Unmarshal(masterMetaBytes, &masterMeta); err != nil {
		// TODO wrap the error
		return nil, errors.Trace(err)
	}
	return &masterMeta, nil
}

func (c *MasterMetadataClient) Store(ctx context.Context, data *libModel.MasterMetaKVData) error {
	key := adapter.MasterMetaKey.Encode(c.masterID)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = c.metaKVClient.Put(ctx, key, string(dataBytes))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// LoadAllMasters loads all job masters from metastore
func (c *MasterMetadataClient) LoadAllMasters(ctx context.Context) ([]*libModel.MasterMetaKVData, error) {
	resp, err := c.metaKVClient.Get(ctx, adapter.MasterMetaKey.Path(), metaclient.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta := make([]*libModel.MasterMetaKVData, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		masterMeta := &libModel.MasterMetaKVData{}
		if err := json.Unmarshal(kv.Value, masterMeta); err != nil {
			return nil, errors.Trace(err)
		}
		meta = append(meta, masterMeta)
	}
	return meta, nil
}

type WorkerMetadataClient struct {
	masterID     libModel.MasterID
	metaKVClient metaclient.KVClient
}

func NewWorkerMetadataClient(
	masterID libModel.MasterID,
	metaClient metaclient.KVClient,
) *WorkerMetadataClient {
	return &WorkerMetadataClient{
		masterID:     masterID,
		metaKVClient: metaClient,
	}
}

func (c *WorkerMetadataClient) LoadAllWorkers(ctx context.Context) (map[libModel.WorkerID]*libModel.WorkerStatus, error) {
	loadPrefix := adapter.WorkerKeyAdapter.Curry(c.masterID)
	resp, err := c.metaKVClient.Get(ctx, loadPrefix.Path(), metaclient.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make(map[libModel.WorkerID]*libModel.WorkerStatus, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		decoded, err := adapter.WorkerKeyAdapter.Decode(string(kv.Key))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(decoded) != 2 {
			// TODO add an error type
			return nil, errors.Errorf("unexpected key: %s", string(kv.Key))
		}

		// NOTE decoded[0] is the master ID.
		workerID := decoded[1]

		workerMetaBytes := kv.Value
		var workerMeta libModel.WorkerStatus
		if err := json.Unmarshal(workerMetaBytes, &workerMeta); err != nil {
			// TODO wrap the error
			return nil, errors.Trace(err)
		}
		ret[workerID] = &workerMeta
	}
	return ret, nil
}

func (c *WorkerMetadataClient) Load(ctx context.Context, workerID libModel.WorkerID) (*libModel.WorkerStatus, error) {
	resp, err := c.metaKVClient.Get(ctx, c.workerMetaKey(workerID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, derror.ErrWorkerNoMeta.GenWithStackByArgs()
	}
	workerMetaBytes := resp.Kvs[0].Value
	var workerMeta libModel.WorkerStatus
	if err := json.Unmarshal(workerMetaBytes, &workerMeta); err != nil {
		// TODO wrap the error
		return nil, errors.Trace(err)
	}

	return &workerMeta, nil
}

func (c *WorkerMetadataClient) Remove(ctx context.Context, id libModel.WorkerID) (bool, error) {
	_, err := c.metaKVClient.Delete(ctx, c.workerMetaKey(id))
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

func (c *WorkerMetadataClient) Store(ctx context.Context, workerID libModel.WorkerID, data *libModel.WorkerStatus) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = c.metaKVClient.Put(ctx, c.workerMetaKey(workerID), string(dataBytes))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *WorkerMetadataClient) MasterID() libModel.MasterID {
	return c.masterID
}

func (c *WorkerMetadataClient) workerMetaKey(id libModel.WorkerID) string {
	return libModel.EncodeWorkerStatusKey(c.masterID, id)
}

// StoreMasterMeta is exposed to job manager for job master meta persistence
func StoreMasterMeta(
	ctx context.Context,
	metaKVClient metaclient.KVClient,
	meta *libModel.MasterMetaKVData,
) error {
	metaClient := NewMasterMetadataClient(meta.ID, metaKVClient)
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
