package dm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopic(t *testing.T) {
	t.Parallel()

	require.Equal(t, "operate-task-message-master-id-task-id", OperateTaskMessageTopic("master-id", "task-id"))
	require.Equal(t, "query-status-request-master-id-task-id", QueryStatusRequestTopic("master-id", "task-id"))
	require.Equal(t, "query-status-response-worker-id-task-id", QueryStatusResponseTopic("worker-id", "task-id"))
}
