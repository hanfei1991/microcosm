package metadata

import (
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
	libModel "github.com/hanfei1991/microcosm/pkg/meta/orm/model"
)

// DDL represents the state of ddls.
// TODO: implement DDL
type DDL struct {
	State
}

// DDLStore manages the state of ddls.
// Write by DDLCoordinator.
type DDLStore struct {
	*TomlStore

	id libModel.MasterID
}

func NewDDLStore(id libModel.MasterID, kvClient kvclient.KVClient) *DDLStore {
	ddlStore := &DDLStore{
		TomlStore: NewTomlStore(kvClient),
		id:        id,
	}
	ddlStore.TomlStore.Store = ddlStore
	return ddlStore
}

func (ddlStore *DDLStore) CreateState() State {
	return &DDL{}
}

// TODO: add ddl key
func (ddlStore *DDLStore) Key() string {
	return ""
}
