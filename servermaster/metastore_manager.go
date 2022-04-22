package servermaster

import (
	"sync"

	"github.com/hanfei1991/microcosm/pkg/errors"
	metacom "github.com/hanfei1991/microcosm/pkg/meta/common"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type MetaStoreManager interface {
	// Register register specify backend store to manager with an unique id
	// id can be some readable identifier, like `meta-test1`.
	// Duplicate id will return an error
	Register(id string, store *metacom.StoreConfigParams) error
	// UnRegister delete an existing backend store
	UnRegister(id string)
	// GetMetaStore get an existing backend store info
	GetMetaStore(id string) *metacom.StoreConfigParams
}

type metaStoreManagerImpl struct {
	// From id to metacom.StoreConfigParams
	id2Store sync.Map
}

func NewMetaStoreManager() MetaStoreManager {
	return &metaStoreManagerImpl{}
}

func (m *metaStoreManagerImpl) Register(id string, store *metacom.StoreConfigParams) error {
	if _, exists := m.id2Store.LoadOrStore(id, store); exists {
		log.L().Error("register metastore fail", zap.Any("metastore", cfg.FrameMetaConf), zap.String("error", "duplicateID"))
		return errors.ErrMetaStoreIDDuplicate.GenWithStackByArgs()
	}

	log.L().Info("register metastore", zap.Any("metastore", cfg.FrameMetaConf))
	return nil
}

func (m *metaStoreManagerImpl) UnRegister(id string) {
	m.id2Store.Delete(id)
	log.L().Info("unregister metastore", zap.Any("metastore", cfg.FrameMetaConf))
}

func (m *metaStoreManagerImpl) GetMetaStore(id string) *metacom.StoreConfigParams {
	if store, exists := m.id2Store.Load(id); exists {
		return store.(*metacom.StoreConfigParams)
	}

	return nil
}

// NewFrameMetaConfig return the default framework metastore config
func NewFrameMetaConfig() *metacom.StoreConfigParams {
	return &metacom.StoreConfigParams{
		StoreID: metacom.FrameMetaID,
		Endpoints: []string{
			metacom.DefaultFrameMetaEndpoints,
		},
	}
}

// NewDefaultUserMetaConfig return the default user metastore config
func NewDefaultUserMetaConfig() *metacom.StoreConfigParams {
	return &metacom.StoreConfigParams{
		StoreID: metacom.DefaultUserMetaID,
		Endpoints: []string{
			metacom.DefaultUserMetaEndpoints,
		},
	}
}
