package dataset

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
)

type record struct {
	RID   string
	Value int
}

func (r *record) ID() string {
	return r.RID
}

func TestDatasetBasics(t *testing.T) {
	mockKV := mock.NewMetaMock()
	dataset := NewDataSet[record, *record](mockKV, adapter.MasterInfoKey)
	err := dataset.Upsert(context.TODO(), &record{
		RID:   "123",
		Value: 123,
	})
	require.NoError(t, err)

	err = dataset.Upsert(context.TODO(), &record{
		RID:   "123",
		Value: 456,
	})
	require.NoError(t, err)

	rec, err := dataset.Get(context.TODO(), "123")
	require.NoError(t, err)
	require.Equal(t, &record{
		RID:   "123",
		Value: 456,
	}, rec)

	err = dataset.Delete(context.TODO(), "123")
	require.NoError(t, err)

	_, err = dataset.Get(context.TODO(), "123")
	require.Error(t, err)
	require.Regexp(t, ".*ErrDatasetEntryNotFound.*", err.Error())
}
