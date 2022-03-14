package storage

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/externalresource/internal"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// Proxy represents a handle to a storage space.
// It is used directly by the business logic to perform
// file-related operations.
type Proxy interface {
	ID() model.ResourceID
	CreateFile(ctx context.Context, path string) (FileWriter, error)
	URI() string
}

// FileWriter supports two methods:
// - Write: classical IO API.
// - Close: close the file and completes the upload if needed.
type FileWriter interface {
	storage.ExternalFileWriter
}

type ProxyImpl struct {
	*internal.BaseProxy
	Storage storage.ExternalStorage
}

func (p *ProxyImpl) CreateFile(ctx context.Context, path string) (FileWriter, error) {
	writer, err := p.Storage.Create(ctx, path)
	if err != nil {
		return nil, err
	}
	return &fileWriter{
		ExternalFileWriter: writer,
		resourceID:         p.ResourceID,
		executorID:         p.ExecutorID,
		masterCli:          p.MasterCli,
	}, nil
}

func (p *ProxyImpl) URI() string {
	return p.Storage.URI()
}
