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

func TestOpAccessors(t *testing.T) {
	t.Parallel()

	op := OpGet(key)
	require.True(t, op.IsGet())

	op = OpPut(key, value)
	require.True(t, op.IsPut())

	op = OpDelete(key)
	require.True(t, op.IsDelete())

	op = OpTxn(nil)
	require.True(t, op.IsTxn())

	nop := NewOp()
	nop.isOptsWithPrefix = true
	require.True(t, nop.IsOptsWithPrefix())

	nop.isOptsWithFromKey = true
	require.True(t, nop.IsOptsWithFromKey())
}

func TestOptions(t *testing.T) {
	t.Parallel()

	op := OpGet(key, WithRange(end))
	require.Equal(t, []byte(end), op.RangeBytes())
	require.False(t, op.IsOptsWithPrefix())
	require.False(t, op.IsOptsWithFromKey())

	op = OpGet(key, WithPrefix())
	require.True(t, op.IsOptsWithPrefix())
	require.Equal(t, []byte("kez"), op.RangeBytes())
	op = OpGet(string([]byte{0xff, 0xff, 0xff}), WithPrefix())
	require.True(t, op.IsOptsWithPrefix())
	require.Equal(t, noPrefixEnd, op.RangeBytes())

	op = OpGet(key, WithFromKey())
	require.False(t, op.IsOptsWithPrefix())
	require.True(t, op.IsOptsWithFromKey())
	require.Equal(t, []byte("\x00"), op.RangeBytes())
}

func TestOptionFail(t *testing.T) {
	t.Parallel()

	op := OpGet(key, WithRange("zz"), WithPrefix())
	require.Error(t, op.CheckValidOp())

	op = OpGet(key, WithRange("zz"), WithFromKey())
	require.Error(t, op.CheckValidOp())

	op = OpGet(key, WithFromKey(), WithPrefix())
	require.Error(t, op.CheckValidOp())

	op = OpDelete(key, WithFromKey(), WithPrefix())
	require.Error(t, op.CheckValidOp())
}
