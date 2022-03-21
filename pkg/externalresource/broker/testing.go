package broker

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/externalresource/manager"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

// LocalBroker is a broker unit-testing other components
// that depend on a Broker.
type LocalBroker struct {
	*Impl

	mu            sync.Mutex
	persistedList []resourcemeta.ResourceID
}

func NewBrokerForTesting(executorID resourcemeta.ExecutorID) *LocalBroker {
	dir, err := ioutil.TempDir("/tmp", "*-localfiles")
	if err != nil {
		log.L().Panic("failed to make tempdir")
	}
	cfg := &storagecfg.Config{Local: &storagecfg.LocalFileConfig{BaseDir: dir}}
	return &LocalBroker{
		Impl: NewBroker(cfg, executorID, manager.NewMockClient()),
	}
}

func (b *LocalBroker) OpenStorage(
	ctx context.Context,
	workerID resourcemeta.WorkerID,
	jobID resourcemeta.JobID,
	resourcePath resourcemeta.ResourceID,
) (Handle, error) {
	h, err := b.Impl.OpenStorage(ctx, workerID, jobID, resourcePath)
	if err != nil {
		return nil, err
	}

	return &brExternalStorageHandleForTesting{parent: b, Handle: h}, nil
}

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
