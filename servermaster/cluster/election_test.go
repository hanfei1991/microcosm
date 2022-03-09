package cluster

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
)

func setUpTest(t *testing.T) (newClient func() *clientv3.Client, close func()) {
	dir := t.TempDir()

	// Change the privilege to prevent a warning.
	err := os.Chmod(dir, 0o700)
	require.NoError(t, err)

	urls, server, err := etcdutils.SetupEmbedEtcd(dir)
	require.NoError(t, err)

	var endpoints []string
	for _, uri := range urls {
		endpoints = append(endpoints, uri.String())
	}

	return func() *clientv3.Client {
			cli, err := clientv3.NewFromURLs(endpoints)
			require.NoError(t, err)
			return cli
		}, func() {
			server.Close()
		}
}

const (
	numMockNodesForCampaignTest = 8
)

func TestEtcdElectionCampaign(t *testing.T) {
	// Sets up an embedded Etcd cluster
	newClient, closeFn := setUpTest(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		wg                     sync.WaitGroup
		leaderCount            atomic.Int32
		accumulatedLeaderCount atomic.Int32
	)

	for i := 0; i < numMockNodesForCampaignTest; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := newClient()
			election, err := NewEtcdElection(ctx, client, nil, EtcdElectionConfig{
				CreateSessionTimeout: 1 * time.Second,
				TTL:                  5,
				Prefix:               "/test-election",
			})
			require.NoError(t, err)

			nodeID := fmt.Sprintf("node-%d", i)
			sessCtx, resignFn, err := election.Campaign(ctx, nodeID, time.Second*5)
			require.NoError(t, err)

			// We have been elected
			newLeaderCount := leaderCount.Add(1)
			require.Equal(t, int32(1), newLeaderCount)

			accumulatedLeaderCount.Add(1)

			// We stay the leader for a while
			time.Sleep(10 * time.Millisecond)

			select {
			case <-sessCtx.Done():
				require.Fail(t, "the session should not have been canceled")
			default:
			}

			newLeaderCount = leaderCount.Sub(1)
			require.Equal(t, int32(0), newLeaderCount)
			resignFn()
		}()
	}

	wg.Wait()
	require.Equal(t, int32(numMockNodesForCampaignTest), accumulatedLeaderCount.Load())
}

// TODO (zixiong) add tests for failure cases
// We need a mock Etcd client.

func TestLeaderCtxCancelPropagate(t *testing.T) {
	newClient, closeFn := setUpTest(t)
	defer closeFn()

	ctx := context.Background()
	client := newClient()
	election, err := NewEtcdElection(ctx, client, nil, EtcdElectionConfig{
		CreateSessionTimeout: 1 * time.Second,
		TTL:                  5,
		Prefix:               "/test-election",
	})
	require.NoError(t, err)

	nodeID := "node-cancel-propagate"
	sessCtx, resignFn, err := election.Campaign(ctx, nodeID, time.Second*5)
	require.NoError(t, err)
	_, cancel := context.WithCancel(sessCtx)
	defer cancel()
	resignFn()
	require.EqualError(t, sessCtx.Err(), "[DFLOW:ErrMasterSessionDone]master session is done")
}
