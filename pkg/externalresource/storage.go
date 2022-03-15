package externalresource

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
)

// Proxy represents a handle to a storage space.
// It is used directly by the business logic to perform
// file-related operations.
type Proxy interface {
	brStorage.ExternalStorage

	ID() model.ResourceID
	CreateFile(ctx context.Context, path string) (FileWriter, error)
}

// FileWriter supports two methods:
// - Write: classical IO API.
// - Close: close the file and completes the upload if needed.
type FileWriter interface {
	brStorage.ExternalFileWriter
}

type ProxyImpl struct {
	*BaseProxy
	brStorage.ExternalStorage
}

func (p *ProxyImpl) CreateFile(ctx context.Context, path string) (FileWriter, error) {
	writer, err := p.ExternalStorage.Create(ctx, path)
	if err != nil {
		return nil, err
	}
	return &FileWriterImpl{
		ExternalFileWriter: writer,
		ResourceID:         p.ResourceID,
		ExecutorID:         p.ExecutorID,
		MasterCli:          p.MasterCli,
	}, nil
}
