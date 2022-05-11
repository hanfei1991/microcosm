package broker

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

type (
	workerIDToResourcesMap = map[libModel.WorkerID]map[resModel.ResourceID]struct{}
	LocalFileManager       struct {
		config storagecfg.LocalFileConfig

		mu                          sync.Mutex
		persistedResourcesByCreator workerIDToResourcesMap
	}
)

func NewLocalFileManager(config storagecfg.LocalFileConfig) *LocalFileManager {
	return &LocalFileManager{
		config:                      config,
		persistedResourcesByCreator: make(workerIDToResourcesMap),
	}
}

func (m *LocalFileManager) CreateResource(
	creator libModel.WorkerID,
	resourceID resModel.ResourceID,
) (resModel.LocalFileResourceDescriptor, error) {
	// Not actually writing the file system for now.
	// Add some logic here when we implement quota.

	return resModel.LocalFileResourceDescriptor{
		BasePath:   m.config.BaseDir,
		Creator:    creator,
		ResourceID: resourceID,
	}, nil
}

func (m *LocalFileManager) GetResource(
	creator libModel.WorkerID,
	resourceID resModel.ResourceID,
) (resModel.LocalFileResourceDescriptor, error) {
	// TODO check whether the resource's directory exists

	return resModel.LocalFileResourceDescriptor{
		BasePath:   m.config.BaseDir,
		Creator:    creator,
		ResourceID: resourceID,
	}, nil
}

func (m *LocalFileManager) RemoveTemporaryFiles(creator libModel.WorkerID) error {
	log.L().Info("Start cleaning temporary files",
		zap.String("worker-id", creator))

	creatorResourcePath := filepath.Join(m.config.BaseDir, creator)

	if _, err := os.Stat(creatorResourcePath); err != nil {
		// The directory not existing is expected if the worker
		// has never created any local file resource.
		if os.IsNotExist(err) {
			log.L().Info("RemoveTemporaryFiles: no local files found for worker",
				zap.String("worker-id", creator))
			return nil
		}

		// Other errors need to be thrown to the caller.
		return derrors.ErrReadLocalFileDirectoryFailed.Wrap(err)
	}

	// Iterates over all resources created by `creator`.
	err := IterOverResourceDirectories(creatorResourcePath, func(resourceID string) error {
		if m.isPersisted(creator, resourceID) {
			// Persisted resources are skipped, as they are NOT temporary.
			return nil
		}

		fullPath := filepath.Join(m.config.BaseDir, creator, resourceID)
		if err := os.RemoveAll(fullPath); err != nil {
			return derrors.ErrCleaningLocalTempFiles.Wrap(err)
		}

		log.L().Info("temporary resource is removed",
			zap.String("resource-id", resourceID),
			zap.String("full-path", fullPath))
		return nil
	})

	log.L().Info("Finished cleaning temporary files",
		zap.String("worker-id", creator))
	return err
}

func (m *LocalFileManager) RemoveResource(creator libModel.WorkerID, resourceID resModel.ResourceID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	resourcePath := filepath.Join(m.config.BaseDir, creator, resourceID)
	if _, err := os.Stat(resourcePath); err != nil {
		if os.IsNotExist(err) {
			log.L().Info("Trying to remove non-existing resource",
				zap.String("creator", creator),
				zap.String("resource-id", resourceID))
			return nil
		}
	}
}

func (m *LocalFileManager) SetPersisted(
	creator libModel.WorkerID,
	resourceID resModel.ResourceID,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	persistedResourceSet, ok := m.persistedResourcesByCreator[creator]
	if !ok {
		persistedResourceSet = make(map[resModel.ResourceID]struct{})
		m.persistedResourcesByCreator[creator] = persistedResourceSet
	}

	persistedResourceSet[resourceID] = struct{}{}
	return
}

// isPersisted returns whether a resource has been persisted.
// DO NOT hold the mu when calling this method.
func (m *LocalFileManager) isPersisted(
	creator libModel.WorkerID,
	resourceID resModel.ResourceID,
) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	persistedResourceSet, ok := m.persistedResourcesByCreator[creator]
	if !ok {
		return false
	}

	_, isPersisted := persistedResourceSet[resourceID]
	return isPersisted
}

func IterOverResourceDirectories(path string, fn func(relPath string) error) error {
	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return derrors.ErrReadLocalFileDirectoryFailed.Wrap(err)
	}

	for _, info := range infos {
		if !info.IsDir() {
			// We skip non-directory files
			continue
		}
		// Note that info.Name() returns the "base name", which
		// is the last part of the file's path.
		name := info.Name()
		if err := fn(name); err != nil {
			return err
		}
	}
	return nil
}
