package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaStoreKey(t *testing.T) {
	testcases := []struct {
		key      string
		expected *DataFlowKey
	}{
		{
			key: "/dataflow/executor/executor-1",
			expected: &DataFlowKey{
				Tp:     DataFlowKeyTypeExecutor,
				NodeID: "executor-1",
			},
		},
	}
	for _, tc := range testcases {
		k := new(DataFlowKey)
		err := k.Parse(tc.key)
		require.Nil(t, err)
		require.Equal(t, tc.expected, k)
		require.Equal(t, tc.key, k.String())
	}
}

func TestMetaStoreKeyParseError(t *testing.T) {
	testCases := []struct {
		key string
		msg string
	}{
		{
			key: "/executor/executor-1",
			msg: ".*invalid metastore key /executor/executor-1",
		},
		{
			key: "/dataflow/allocator/allocator-1",
			msg: ".*invalid metastore key type /allocator/allocator-1",
		},
	}
	for _, tc := range testCases {
		k := new(DataFlowKey)
		err := k.Parse(tc.key)
		require.NotNil(t, err)
		require.Regexp(t, tc.msg, err.Error())
	}
}
