package metadata

import (
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
	libModel "github.com/hanfei1991/microcosm/pkg/meta/orm/model"
)

// MetaData is the metadata of dm.
type MetaData struct {
	jobStore *JobStore
	ddlStore *DDLStore
}

func NewMetaData(id libModel.WorkerID, kvClient kvclient.KVClient) *MetaData {
	return &MetaData{
		jobStore: NewJobStore(id, kvClient),
		ddlStore: NewDDLStore(id, kvClient),
	}
}

func (m *MetaData) JobStore() *JobStore {
	return m.jobStore
}

func (m *MetaData) DDLStore() *DDLStore {
	return m.ddlStore
}
