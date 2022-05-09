package broker

import (
	libModel "github.com/hanfei1991/microcosm/lib/model"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

type LocalFileManager struct {
	config storagecfg.LocalFileConfig
}

func (m *LocalFileManager) CreateResource(
	creator libModel.WorkerID,
	resourceID resourcemeta.ResourceID,
) error {
	panic("implement me")
}

func (m *LocalFileManager) GetPathForResource(
	creator libModel.WorkerID,
	resourceID resourcemeta.ResourceID,
) (string, error) {
	panic("implement me")
}

func (m *LocalFileManager) RemoveTemporaryFiles(creator libModel.WorkerID) error {
	//TODO implement me
	panic("implement me")
}

func (m *LocalFileManager) RemoveResource(resourceID resourcemeta.ResourceID) error {
	//TODO implement me
	panic("implement me")
}
