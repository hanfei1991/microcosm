package broker

import (
	"context"
	"path/filepath"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"

	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

func getWorkerDir(config *storagecfg.Config, workerID resModel.WorkerID) string {
	return filepath.Join(config.Local.BaseDir, workerID)
}

func newBrStorageForLocalFile(filePath string) (brStorage.ExternalStorage, error) {
	backend, err := brStorage.ParseBackend(filePath, nil)
	if err != nil {
		return nil, err
	}
	ls, err := brStorage.New(context.Background(), backend, nil)
	if err != nil {
		return nil, derrors.ErrFailToCreateExternalStorage.Wrap(err)
	}
	return ls, nil
}
