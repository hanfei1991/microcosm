package externalresource

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

type ID = string

// FileWriter supports two methods:
// - Write: classical IO API.
// - Close: close the file and completes the upload if needed.
type FileWriter interface {
	storage.ExternalFileWriter
}

type fileWriter struct {
	storage.ExternalFileWriter
	resourceID ID
	executorID string
	workerID   model.WorkerID
	masterCli  client.MasterClient
}

func (f *fileWriter) Close(ctx context.Context) error {
	err := f.ExternalFileWriter.Close(ctx)
	if err != nil {
		return err
	}
	// failing here will generate trash files, need clean it
	resp, err := f.masterCli.UpdateResource(ctx, &pb.UpdateResourceRequest{
		WorkerId:   f.workerID,
		ResourceId: string(f.resourceID),
		// We are using the timeout lease as the default lease for persistence.
		// The alternative may be a job lease.
		LeaseType: pb.ResourceLeaseType_LeaseTimeout,
	})
	if err != nil {
		return errors.New(resp.Error.String())
	}
	return nil
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

type proxy struct {
	resourceID model.ResourceID
	executorID string
	workerID   string
	jobID      string

	masterCli client.MasterClient
	storage   storage.ExternalStorage
}

func (p proxy) ID() ID {
	return p.resourceID
}

func (p proxy) CreateFile(ctx context.Context, path string) (FileWriter, error) {
	writer, err := p.storage.Create(ctx, path)
	if err != nil {
		return nil, err
	}
	return &fileWriter{
		ExternalFileWriter: writer,
		resourceID:         p.resourceID,
		executorID:         p.executorID,
		masterCli:          p.masterCli,
	}, nil
}

func newProxy(ctx context.Context, pathPrefix, id string) (*proxy, error) {
	storagePath := filepath.Join(pathPrefix, id)
	// only support local disk now
	backend, err := storage.ParseBackend(storagePath, nil)
	if err != nil {
		return nil, err
	}
	s, err := storage.New(ctx, backend, nil)
	if err != nil {
		return nil, err
	}
	return &proxy{
		resourceID: ID(id),
		storage:    s,
	}, nil
}

// Broker is singleton per executor, it communicates with Manager of server master.
type Broker struct {
	// map[ID]struct{}
	allocated        sync.Map
	collectLocalOnce sync.Once

	executorID string
	pathPrefix string
	masterCli  client.MasterClient // nil when in test
}

func NewBroker(executorID, pathPrefix string, masterCli client.MasterClient) *Broker {
	return &Broker{
		executorID: executorID,
		pathPrefix: pathPrefix,
		masterCli:  masterCli,
	}
}

func (b *Broker) NewProxyForWorker(
	ctx context.Context,
	resourceID model.ResourceID,
	workerID model.WorkerID,
	jobID model.JobID) (Proxy, error) {

	resp, err := b.masterCli.CreateResource(ctx, &pb.CreateResourceRequest{
		ResourceId: resourceID,
		ExecutorId: b.executorID,
		JobId:      jobID,
		WorkerId:   workerID,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch resp.Error.ErrorCode {
	case pb.ResourceErrorCode_ResourceOK:
		break
	case pb.ResourceErrorCode_ResourceDuplicate:
		return nil, derror.ErrDuplicateResources.GenWithStackByArgs(resourceID)
	default:
		return nil, derror.ErrCreateResourceFailed.GenWithStackByArgs(resp.GetError().String())
	}
	if resp.Error.BaseError != nil {
		return nil, derror.ErrCreateResourceFailed.GenWithStackByArgs(resp.GetError().String())
	}

	p, err := newProxy(ctx, b.pathPrefix, resourceID)
	if err != nil {
		return nil, err
	}
	// we only need one request for one resource proxy (one folder)
	p.masterCli = &ignoreAfterSuccClient{
		MasterClient: b.masterCli,
	}
	p.executorID = b.executorID
	b.allocated.Store(ID(resourceID), struct{}{})
	return p, nil
}

// Remove the allocated information and local resource folder (if applicable) for
// the given ID. Manager should make sure no worker is using the resource.
func (b *Broker) Remove(id string) {
	folder := filepath.Join(b.pathPrefix, id)
	err := os.RemoveAll(folder)
	if err != nil {
		log.L().Error("failed to remove resource folder",
			zap.String("folder", folder), zap.Error(err))
	}
	b.allocated.Delete(ID(id))
}

func (b *Broker) AllocatedIDs() []string {
	b.collectLocalOnce.Do(b.collectLocal)

	var ids []string
	b.allocated.Range(func(k, v interface{}) bool {
		ids = append(ids, string(k.(ID)))
		return true
	})
	return ids
}

func (b *Broker) collectLocal() {
	entries, err := ioutil.ReadDir(b.pathPrefix)
	if err != nil {
		log.L().Error("failed to read local resources", zap.Error(err))
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		b.allocated.Store(ID(entry.Name()), struct{}{})
	}
}

type MockProxyWithMasterCli struct {
	*proxy
	MockMasterCli *client.MockServerMasterClient
}

func NewMockProxy(id string) MockProxyWithMasterCli {
	p, err := newProxy(context.TODO(), "unit_test_resources", id)
	if err != nil {
		log.L().Panic("failed in NewMockProxy",
			zap.String("resourceID", id),
			zap.Error(err))
	}
	mockMasterCli := &client.MockServerMasterClient{}
	p.masterCli = mockMasterCli
	return MockProxyWithMasterCli{
		proxy:         p,
		MockMasterCli: mockMasterCli,
	}
}
