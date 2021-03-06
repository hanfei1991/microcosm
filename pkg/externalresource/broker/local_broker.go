package broker

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gogo/status"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/manager"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

// LocalBroker is a broker unit-testing other components
// that depend on a Broker.
type LocalBroker struct {
	*DefaultBroker

	clientMu sync.Mutex
	client   *manager.MockClient

	mu            sync.Mutex
	persistedList []resourcemeta.ResourceID
}

// NewBrokerForTesting creates a LocalBroker instance for testing only
func NewBrokerForTesting(executorID resourcemeta.ExecutorID) *LocalBroker {
	dir, err := ioutil.TempDir("/tmp", "*-localfiles")
	if err != nil {
		log.L().Panic("failed to make tempdir")
	}
	cfg := &storagecfg.Config{Local: &storagecfg.LocalFileConfig{BaseDir: dir}}
	client := manager.NewWrappedMockClient()
	return &LocalBroker{
		DefaultBroker: NewBroker(cfg, executorID, client),
		client:        client.GetLeaderClient().(*manager.MockClient),
	}
}

// OpenStorage wraps broker.OpenStorage
func (b *LocalBroker) OpenStorage(
	ctx context.Context,
	workerID resourcemeta.WorkerID,
	jobID resourcemeta.JobID,
	resourcePath resourcemeta.ResourceID,
) (Handle, error) {
	b.clientMu.Lock()
	defer b.clientMu.Unlock()

	st := status.New(codes.NotFound, "resource manager error")

	b.client.On("QueryResource", mock.Anything, &pb.QueryResourceRequest{ResourceId: resourcePath}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), st.Err())
	defer func() {
		b.client.ExpectedCalls = nil
	}()
	h, err := b.DefaultBroker.OpenStorage(ctx, workerID, jobID, resourcePath)
	if err != nil {
		return nil, err
	}

	return &brExternalStorageHandleForTesting{parent: b, Handle: h}, nil
}

// AssertPersisted checks resource is in persisted list
func (b *LocalBroker) AssertPersisted(t *testing.T, id resourcemeta.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	require.Contains(t, b.persistedList, id)
}

func (b *LocalBroker) appendPersistRecord(id resourcemeta.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.persistedList = append(b.persistedList, id)
}

// AssertFileExists checks lock file exists
func (b *LocalBroker) AssertFileExists(
	t *testing.T,
	workerID resourcemeta.WorkerID,
	resourceID resourcemeta.ResourceID,
	fileName string,
) {
	suffix := strings.TrimPrefix(resourceID, "/local/")
	filePath := filepath.Join(b.config.Local.BaseDir, workerID, suffix, fileName)
	require.FileExists(t, filePath)
}

type brExternalStorageHandleForTesting struct {
	Handle
	parent *LocalBroker
}

func (h *brExternalStorageHandleForTesting) Persist(ctx context.Context) error {
	h.parent.appendPersistRecord(h.ID())
	return nil
}
