package broker

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

type Handle interface {
	ID() resourcemeta.ResourceID
	BrExternalStorage() brStorage.ExternalStorage
	Persist(ctx context.Context) error
	Discard(ctx context.Context) error
}

// BrExternalStorageHandle contains a brStorage.ExternalStorage.
// It helps Dataflow Engine reuse the external storage facilities
// implemented in Br.
type BrExternalStorageHandle struct {
	id         resourcemeta.ResourceID
	jobID      resourcemeta.JobID
	workerID   resourcemeta.WorkerID
	executorID resourcemeta.ExecutorID

	inner    brStorage.ExternalStorage
	accessor *resourcemeta.MetadataAccessor
}

func (h *BrExternalStorageHandle) ID() resourcemeta.ResourceID {
	return h.id
}

func (h *BrExternalStorageHandle) BrExternalStorage() brStorage.ExternalStorage {
	return h.inner
}

func (h *BrExternalStorageHandle) Persist(ctx context.Context) error {
	ok, err := h.accessor.CreateResource(ctx, &resourcemeta.ResourceMeta{
		ID:       h.id,
		Job:      h.jobID,
		Worker:   h.workerID,
		Executor: h.executorID,
		Deleted:  false,
	})
	if err != nil {
		return err
	}
	if !ok {
		return derror.ErrDuplicateResourceID.GenWithStackByArgs(h.id)
	}
	return nil
}

func (h *BrExternalStorageHandle) Discard(ctx context.Context) error {
	// TODO implement me
	return nil
}

type Factory struct {
	config   *storagecfg.Config
	accessor *resourcemeta.MetadataAccessor
}

func (f *Factory) NewHandleForLocalFile(
	ctx context.Context,
	workerID resourcemeta.WorkerID,
	resourceID resourcemeta.ResourceID,
) (Handle, error) {
	pathSuffix, err := getPathSuffix(resourcemeta.ResourceTypeLocalFile, resourceID)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(getWorkerDir(f.config, workerID), pathSuffix)
	log.L().Info("Using local storage with path", zap.String("path", filePath))

	ls, err := brStorage.NewLocalStorage(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &BrExternalStorageHandle{
		inner:    ls,
		accessor: f.accessor,
	}, nil
}

func getPathSuffix(prefix resourcemeta.ResourceType, path resourcemeta.ResourceID) (string, error) {
	if !strings.HasPrefix(path, "/"+string(prefix)+"/") {
		return "", derror.ErrUnexpectedResourcePath.GenWithStackByArgs(path)
	}
	return strings.TrimSuffix(path, "/"+string(prefix)+"/"), nil
}

func getWorkerDir(config *storagecfg.Config, workerID resourcemeta.WorkerID) string {
	return filepath.Join(config.Local.BaseDir, workerID)
}
