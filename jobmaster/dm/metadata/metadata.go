package metadata

import (
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/metadata"
)

// MetaData is the metadata of dm.
type MetaData struct {
	jobStore *JobStore
	ddlStore *DDLStore
}

func NewMetaData(id lib.MasterID, kvClient metadata.MetaKV) *MetaData {
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
