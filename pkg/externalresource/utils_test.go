package externalresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
)

func TestIgnoreAfterSuccClient(t *testing.T) {
	mockCli := &client.MockServerMasterClient{}
	c := &ignoreAfterSuccClient{MasterClient: mockCli}

	ctx := context.Background()
	req := &pb.UpdateResourceRequest{}

	// mock expect once
	mockCli.Mock.On("UpdateResource", mock.Anything, mock.Anything).
		Return(&pb.UpdateResourceResponse{
			Error: &pb.ResourceError{ErrorCode: pb.ResourceErrorCode_ResourceOK},
		}, nil)

	// call twice
	check := func(resp *pb.UpdateResourceResponse, err error) {
		require.NoError(t, err)
		require.Equal(t, resp.Error.ErrorCode, pb.ResourceErrorCode_ResourceOK)
	}
	check(c.UpdateResource(ctx, req))
	check(c.UpdateResource(ctx, req))
}
