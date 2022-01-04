package p2p

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"
	p2pImpl "github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func makeListenerForServerTests(t *testing.T) (l net.Listener, addr string) {
	port := freeport.GetPort()
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	l, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	return
}

// read only
var clientConfigForUnitTesting = &p2pImpl.MessageClientConfig{
	SendChannelSize:         0, // unbuffered channel to make tests more reliable
	BatchSendInterval:       time.Second,
	MaxBatchBytes:           math.MaxInt64,
	MaxBatchCount:           math.MaxInt64,
	RetryRateLimitPerSecond: 999.0,
	ClientVersion:           "v5.4.0", // a fake version
	AdvertisedAddr:          "fake-addr:8300",
}

func TestMessageRPCServiceBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	l, addr := makeListenerForServerTests(t)
	messageSrvc, err := NewMessageRPCService("test-node-1", &security.Credential{} /* no TLS */)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := messageSrvc.Serve(ctx, l)
		require.Error(t, err)
		require.Regexp(t, ".*canceled.*", err.Error())
	}()

	var called atomic.Bool
	handlerManager := messageSrvc.MakeHandlerManager()
	ok, err := handlerManager.RegisterHandler(ctx, "test-topic-1", &msgContent{}, func(sender NodeID, value MessageValue) error {
		require.Equal(t, "test-client-1", sender)
		require.IsType(t, &msgContent{}, value)
		require.False(t, called.Swap(true))
		return nil
	})
	require.NoError(t, err)
	require.True(t, ok)

	client := p2pImpl.NewMessageClient("test-client-1", clientConfigForUnitTesting)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(ctx, "tcp", addr, "test-node-1", &security.Credential{} /* no TLS */)
		require.Error(t, err)
		require.Regexp(t, ".*canceled.*", err.Error())
	}()

	_, err = client.SendMessage(ctx, "test-topic-1", &msgContent{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return called.Load()
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}
