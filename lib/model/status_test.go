package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTerminateState(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		code     WorkerStatusCode
		expected bool
	}{
		{WorkerStatusNormal, false},
		{WorkerStatusCreated, false},
		{WorkerStatusInit, false},
		{WorkerStatusError, true},
		{WorkerStatusFinished, true},
		{WorkerStatusStopped, true},
	}
	s := &WorkerStatus{}
	for _, tc := range testCases {
		s.Code = tc.code
		require.Equal(t, tc.expected, s.InTerminateState())
	}
}
