package types

import (
	"context"
	"path/filepath"

	"github.com/hanfei1991/microcosm/pkg/externalresource/internal"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storage"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
)

const (
	ResourceTypeLocalFile = "local"

	// LocalFsPrefix configs the directory under which
	// all local file resources are stored.
	// TODO pass a config option from the executor.
	LocalFsPrefix = "/tmp/dataflow/files/"
)

type localFileProxy struct {
	*internal.BaseProxy
}

// LocalFileType implements methods necessary for manage
// local on-disk files.
type LocalFileType struct{}

func (l *LocalFileType) IsBoundToExecutor() bool {
	return true
}

// CleanUp is called on the server master when a local file
// needs to be cleaned up. This implementation will ask the executor
// by gRPC to remove the corresponding file.
func (l *LocalFileType) CleanUp(ctx context.Context, id model.ResourceID) error {
	panic("implement me")
}

// CreateStorage is called on the executor when a worker tries to access a
// local file storage space.
func (l *LocalFileType) CreateStorage(
	ctx context.Context,
	baseProxy *internal.BaseProxy,
	suffix string,
) (storage.Proxy, error) {
	storagePath := filepath.Join(LocalFsPrefix, suffix)
	backend, err := brStorage.ParseBackend(storagePath, nil)
	if err != nil {
		return nil, err
	}
	s, err := brStorage.New(ctx, backend, nil)
	if err != nil {
		return nil, err
	}

	return &storage.ProxyImpl{
		BaseProxy: baseProxy,
		Storage:   s,
	}, nil
}
