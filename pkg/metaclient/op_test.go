package metaclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	key   string = "key"
	value string = "value"
	end   string = "value_end"
)

func TestOpAccessorsAndMutators(t *testing.T) {
	t.Parallel()

	op, _ := OpGet(key)
	require.True(t, op.IsGet())

	op, _ = OpPut(key, value)
	require.True(t, op.IsPut())

	op, _ = OpDelete(key)
	require.True(t, op.IsDelete())

	op, _ = OpTxn(nil)
	require.True(t, op.IsTxn())

	nop := NewOp()
	nop.WithKeyBytes([]byte(key))
	require.Equal(t, []byte(key), nop.KeyBytes())

	nop.WithValueBytes([]byte(value))
	require.Equal(t, []byte(value), nop.ValueBytes())

	nop.WithRangeBytes([]byte(end))
	require.Equal(t, []byte(end), nop.RangeBytes())

	nop.WithTTL(11)
	require.Equal(t, int64(11), nop.TTL())

	nop.WithSort(SortOption{
		Target: SortByKey,
		Order:  SortDescend,
	})
	require.Equal(t, SortByKey, nop.Sort().Target)
	require.Equal(t, SortDescend, nop.Sort().Order)

	nop.WithLimit(11)
	require.Equal(t, int64(11), nop.Limit())

	nop.isOptsWithPrefix = true
	require.True(t, nop.IsOptsWithPrefix())

	nop.isOptsWithFromKey = true
	require.True(t, nop.IsOptsWithFromKey())
}

func TestOptions(t *testing.T) {
	t.Parallel()

	op, err := OpGet(key, WithTTL(11), WithLimit(12),
		WithSort(SortByKey, SortAscend))
	require.Nil(t, err)
	require.Equal(t, int64(11), op.TTL())
	require.Equal(t, int64(12), op.Limit())
	require.Equal(t, SortByKey, op.Sort().Target)
	require.Equal(t, SortAscend, op.Sort().Order)

	op, err = OpGet(key, WithRange(end))
	require.Nil(t, err)
	require.Equal(t, []byte(end), op.RangeBytes())
	require.False(t, op.IsOptsWithPrefix())
	require.False(t, op.IsOptsWithFromKey())

	op, err = OpGet(key, WithPrefix())
	require.Nil(t, err)
	require.True(t, op.IsOptsWithPrefix())
	require.Equal(t, []byte("kez"), op.RangeBytes())
	op, err = OpGet(string([]byte{0xff, 0xff, 0xff}), WithPrefix())
	require.Nil(t, err)
	require.True(t, op.IsOptsWithPrefix())
	require.Equal(t, noPrefixEnd, op.RangeBytes())

	op, err = OpGet(key, WithFromKey())
	require.Nil(t, err)
	require.False(t, op.IsOptsWithPrefix())
	require.True(t, op.IsOptsWithFromKey())
	require.Equal(t, []byte("\x00"), op.RangeBytes())
}

func TestOptionFail(t *testing.T) {
	t.Parallel()

	_, err := OpDelete(key, WithSort(SortByKey, SortAscend))
	require.Error(t, err)

	_, err = OpPut(key, value, WithLimit(11))
	require.Error(t, err)

	_, err = OpPut(key, value, WithSort(SortByKey, SortAscend))
	require.Error(t, err)
}
