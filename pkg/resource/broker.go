package resource

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type ID string

type FileWriter interface {
	storage.ExternalFileWriter
}

// Proxy is assigned to a worker so worker can create the resource files and notify
// the dataflow engine.
type Proxy interface {
	// ID identifies the resources that one worker created, to provide cross-worker
	// constrains.
	ID() ID
	// CreateFile can create a FileWriter for writing at the given path under current
	// ID.
	CreateFile(ctx context.Context, path string) (FileWriter, error)
}

type Broker struct {
	// TODO: report it to ResourceManager
	allocated map[ID]struct{}
}

var DefaultBroker Broker

type proxy struct {
	id      ID
	storage storage.ExternalStorage
}

func (p proxy) ID() ID {
	return p.id
}

func (p proxy) CreateFile(ctx context.Context, path string) (FileWriter, error) {
	return p.storage.Create(ctx, path)
}

func (b *Broker) NewProxyForWorker(ctx context.Context, id string) (Proxy, error) {
	// only support local disk now
	backend, err := storage.ParseBackend("./resources/"+id, nil)
	if err != nil {
		return nil, err
	}
	s, err := storage.New(ctx, backend, nil)
	if err != nil {
		return nil, err
	}
	b.allocated[ID(id)] = struct{}{}
	return &proxy{
		id:      ID(id),
		storage: s,
	}, nil
}

func NewMockProxy(id string) Proxy {
	backend, err := storage.ParseBackend("./unit_test_resources/"+id, nil)
	if err != nil {
		log.L().Panic("failed to parse backend",
			zap.String("id", id),
			zap.Error(err))
	}
	s, err := storage.New(context.TODO(), backend, nil)
	if err != nil {
		log.L().Panic("failed to create storage",
			zap.String("id", id),
			zap.Error(err))
	}
	return &proxy{
		id:      ID(id),
		storage: s,
	}
}