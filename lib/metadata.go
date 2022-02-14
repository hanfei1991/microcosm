package lib

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"go.etcd.io/etcd/clientv3"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
)

type MasterMetadataClient struct {
	masterID     MasterID
	metaKVClient metadata.MetaKV
}

func NewMasterMetadataClient(masterID MasterID, metaKVClient metadata.MetaKV) *MasterMetadataClient {
	return &MasterMetadataClient{
		masterID:     masterID,
		metaKVClient: metaKVClient,
	}
}

func (c *MasterMetadataClient) Load(ctx context.Context) (*MasterMetaKVData, error) {
	key := adapter.MasterMetaKey.Encode(c.masterID)
	rawResp, err := c.metaKVClient.Get(ctx, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := rawResp.(*clientv3.GetResponse)
	if len(resp.Kvs) == 0 {
		// TODO refine handling the situation where the mata key does not exist at this point
		masterMeta := &MasterMetaKVData{
			ID: c.masterID,
		}
		return masterMeta, nil
	}
	masterMetaBytes := resp.Kvs[0].Value
	var masterMeta MasterMetaKVData
	if err := json.Unmarshal(masterMetaBytes, &masterMeta); err != nil {
		// TODO wrap the error
		return nil, errors.Trace(err)
	}
	return &masterMeta, nil
}

func (c *MasterMetadataClient) Store(ctx context.Context, data *MasterMetaKVData) error {
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

func (c *MasterMetadataClient) GenerateEpoch(ctx context.Context) (Epoch, error) {
	rawResp, err := c.metaKVClient.Get(ctx, "/fake-key")
	if err != nil {
		return 0, errors.Trace(err)
	}

	resp := rawResp.(*clientv3.GetResponse)
	return resp.Header.Revision, nil
}

type WorkerMetadataClient struct {
	masterID     MasterID
	workerID     WorkerID
	metaKVClient metadata.MetaKV
	extTpi       interface{}
}

func NewWorkerMetadataClient(
	masterID MasterID,
	workerID WorkerID,
	metaClient metadata.MetaKV,
	extTpi interface{},
) *WorkerMetadataClient {
	return &WorkerMetadataClient{
		masterID:     masterID,
		workerID:     workerID,
		metaKVClient: metaClient,
		extTpi:       extTpi,
	}
}

func (c *WorkerMetadataClient) Load(ctx context.Context) (*WorkerStatus, error) {
	rawResp, err := c.metaKVClient.Get(ctx, c.workerMetaKey())
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := rawResp.(*clientv3.GetResponse)
	if len(resp.Kvs) == 0 {
		return nil, derror.ErrWorkerNoMeta.GenWithStackByArgs()
	}
	workerMetaBytes := resp.Kvs[0].Value
	var workerMeta WorkerStatus
	if err := json.Unmarshal(workerMetaBytes, &workerMeta); err != nil {
		// TODO wrap the error
		return nil, errors.Trace(err)
	}

	if err := workerMeta.fillExt(c.extTpi); err != nil {
		return nil, errors.Trace(err)
	}
	return &workerMeta, nil
}

func (c *WorkerMetadataClient) Store(ctx context.Context, data *WorkerStatus) error {
	if err := data.marshalExt(); err != nil {
		return errors.Trace(err)
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = c.metaKVClient.Put(ctx, c.workerMetaKey(), string(dataBytes))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *WorkerMetadataClient) MasterID() MasterID {
	return c.masterID
}

func (c *WorkerMetadataClient) WorkerID() WorkerID {
	return c.workerID
}

func (c *WorkerMetadataClient) workerMetaKey() string {
	return adapter.WorkerKeyAdapter.Encode(c.masterID, c.workerID)
}
