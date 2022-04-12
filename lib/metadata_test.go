package lib

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
)

func TestMasterMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	metaKVClient := mockkv.NewMetaMock()
	meta := []*MasterMetaKVData{
		{
			ID: JobManagerUUID,
			Tp: JobManager,
		},
		{
			ID: "master-1",
			Tp: FakeJobMaster,
		},
		{
			ID: "master-2",
			Tp: FakeJobMaster,
		},
	}
	for _, data := range meta {
		cli := NewMasterMetadataClient(data.ID, metaKVClient)
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}
	cli := NewMasterMetadataClient("job-manager", metaKVClient)
	masters, err := cli.LoadAllMasters(ctx)
	require.Nil(t, err)
	require.Len(t, masters, 2)
	for _, master := range masters {
		require.Equal(t, FakeJobMaster, master.Tp)
	}
}

func TestOperateMasterMetadata(t *testing.T) {
	t.Parallel()
	var (
		ctx          = context.Background()
		metaKVClient = mockkv.NewMetaMock()
		addr1        = "127.0.0.1:10000"
		addr2        = "127.0.0.1:10001"
		meta         = &MasterMetaKVData{
			ID:         "master-1",
			Tp:         FakeJobMaster,
			Addr:       addr1,
			StatusCode: libModel.MasterStatusInit,
		}
	)
	loadMeta := func() *MasterMetaKVData {
		cli := NewMasterMetadataClient(meta.ID, metaKVClient)
		meta, err := cli.Load(ctx)
		require.NoError(t, err)
		return meta
	}

	// persist master meta for the first time
	err := StoreMasterMeta(ctx, metaKVClient, meta)
	require.NoError(t, err)
	require.Equal(t, addr1, loadMeta().Addr)

	// overwrite master meta
	meta.Addr = addr2
	err = StoreMasterMeta(ctx, metaKVClient, meta)
	require.NoError(t, err)
	require.Equal(t, addr2, loadMeta().Addr)

	err = DeleteMasterMeta(ctx, metaKVClient, meta.ID)
	require.NoError(t, err)
	// meta is not found in metastore, load meta will return a new master meta
	require.Equal(t, "", loadMeta().Addr)
	require.Equal(t, libModel.MasterStatusUninit, loadMeta().StatusCode)
}

func TestLoadAllWorkers(t *testing.T) {
	t.Parallel()

	metaKVClient := mockkv.NewMetaMock()
	workerMetaClient := NewWorkerMetadataClient("master-1", metaKVClient)

	// Using context.Background() since there is no risk that
	// the mock KV might time out.
	err := workerMetaClient.Store(context.Background(), "worker-1", &libModel.WorkerStatus{
		Code:         libModel.WorkerStatusInit,
		ErrorMessage: "test-1",
		ExtBytes:     []byte("ext-bytes-1"),
	})
	require.NoError(t, err)

	err = workerMetaClient.Store(context.Background(), "worker-2", &libModel.WorkerStatus{
		Code:         libModel.WorkerStatusNormal,
		ErrorMessage: "test-2",
		ExtBytes:     []byte("ext-bytes-2"),
	})
	require.NoError(t, err)

	err = workerMetaClient.Store(context.Background(), "worker-3", &libModel.WorkerStatus{
		Code:         libModel.WorkerStatusFinished,
		ErrorMessage: "test-3",
		ExtBytes:     []byte("ext-bytes-3"),
	})
	require.NoError(t, err)

	workerStatuses, err := workerMetaClient.LoadAllWorkers(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[libModel.WorkerID]*libModel.WorkerStatus{
		"worker-1": {
			Code:         libModel.WorkerStatusInit,
			ErrorMessage: "test-1",
			ExtBytes:     []byte("ext-bytes-1"),
		},
		"worker-2": {
			Code:         libModel.WorkerStatusNormal,
			ErrorMessage: "test-2",
			ExtBytes:     []byte("ext-bytes-2"),
		},
		"worker-3": {
			Code:         libModel.WorkerStatusFinished,
			ErrorMessage: "test-3",
			ExtBytes:     []byte("ext-bytes-3"),
		},
	}, workerStatuses)
}
