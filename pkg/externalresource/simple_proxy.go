package externalresource

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// SimpleProxy is a naive implementation for Proxy.
// It is designed for testing purpose ONLY.
type SimpleProxy struct {
	id model.ResourceID
	*storage.LocalStorage
}

func NewSimpleProxy(id model.ResourceID, basePath string) (*SimpleProxy, error) {
	ls, err := storage.NewLocalStorage(basePath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &SimpleProxy{
		id:           id,
		LocalStorage: ls,
	}, nil
}

func (s *SimpleProxy) ID() model.ResourceID {
	return s.id
}

func (s *SimpleProxy) CreateFile(ctx context.Context, path string) (FileWriter, error) {
	f, err := s.LocalStorage.Create(ctx, path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &FileWriterImpl{
		ExternalFileWriter: f,
		ResourceID:         s.id,
	}, nil
}
