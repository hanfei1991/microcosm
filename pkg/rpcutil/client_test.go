package rpcutil

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type Request struct{}
type Response struct{}

var (
	req *Request = nil
)

type mockRPCClient struct {
	cnt int
}

func (c *mockRPCClient) MockRPC(ctx context.Context, req *Request, opts ...grpc.CallOption) (*Response, error) {
	c.cnt++
	return nil, nil
}

func (c *mockRPCClient) MockFailRPC(ctx context.Context, req *Request, opts ...grpc.CallOption) (*Response, error) {
	c.cnt++
	return nil, errors.New("mock fail")
}

var testClient = &mockRPCClient{}

func mockDail(context.Context, string) (*clientHolder[*mockRPCClient], error) {
	return &clientHolder[*mockRPCClient]{
		client: testClient,
	}, nil
}

func TestFailoverRpcClients(t *testing.T) {
	ctx := context.Background()
	clients, err := NewFailoverRpcClients(ctx, []string{"url1", "url2"}, mockDail)
	require.NoError(t, err)
	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockRPC)
	require.NoError(t, err)
	require.Equal(t, 1, testClient.cnt)

	// reset
	testClient.cnt = 0
	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockFailRPC)
	require.Error(t, err)
	require.Equal(t, 2, testClient.cnt)

	clients.UpdateClients(ctx, []string{"url1", "url2", "url3"}, "")
	testClient.cnt = 0
	_, err = DoFailoverRPC(ctx, clients, req, (*mockRPCClient).MockFailRPC)
	require.Error(t, err)
	require.Equal(t, 3, testClient.cnt)
}
