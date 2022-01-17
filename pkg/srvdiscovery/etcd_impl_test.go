package srvdiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func init() {
	// initialized the logger to make genEmbedEtcdConfig working.
	err := log.InitLogger(&log.Config{})
	if err != nil {
		panic(err)
	}
}

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func prepareEtcd(t *testing.T, name string) (*embed.Etcd, *clientv3.Client, func() /* cleanup function */) {
	dir, err := ioutil.TempDir("", name)
	require.Nil(t, err)

	masterAddr := allocTempURL(t)
	advertiseAddr := masterAddr
	cfgCluster := &etcdutils.ConfigParams{}
	cfgCluster.Name = name
	cfgCluster.DataDir = dir
	cfgCluster.PeerUrls = "http://" + allocTempURL(t)
	cfgCluster.Adjust("", embed.ClusterStateFlagNew)

	cfgClusterEtcd := etcdutils.GenEmbedEtcdConfigWithLogger("info")
	cfgClusterEtcd, err = etcdutils.GenEmbedEtcdConfig(cfgClusterEtcd, masterAddr, advertiseAddr, cfgCluster)
	require.Nil(t, err)

	etcd, err := etcdutils.StartEtcd(cfgClusterEtcd, nil, nil, time.Minute)
	require.Nil(t, err)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{advertiseAddr},
		DialTimeout: 3 * time.Second,
	})
	require.Nil(t, err)

	cleanFn := func() {
		os.RemoveAll(dir)
		etcd.Close()
	}

	return etcd, client, cleanFn
}

func TestEtcdDiscoveryAPI(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	_, client, cleanFn := prepareEtcd(t, "test1")
	defer cleanFn()

	initSrvs := []struct {
		uuid string
		addr string
	}{
		{"uuid-1", "127.0.0.1:10001"},
		{"uuid-2", "127.0.0.1:10002"},
		{"uuid-3", "127.0.0.1:10003"},
	}

	updateSrvs := []struct {
		del  bool
		uuid string
		addr string
	}{
		{true, "uuid-1", "127.0.0.1:10001"},
		{false, "uuid-4", "127.0.0.1:10004"},
		{false, "uuid-5", "127.0.0.1:10005"},
	}

	for _, srv := range initSrvs {
		key := adapter.ServiceAddrAdapter.Encode(srv.uuid)
		value, err := json.Marshal(&ServiceResource{Addr: srv.addr})
		require.Nil(t, err)
		_, err = client.Put(ctx, key, string(value))
		require.Nil(t, err)
	}
	tickDur := 50 * time.Millisecond
	d := NewEtcdSrvDiscovery(client, tickDur)
	snapshot, err := d.Snapshot(ctx, true /*updateCache*/)
	require.Nil(t, err)
	require.Equal(t, 3, len(snapshot))
	require.Contains(t, snapshot, "uuid-1")

	for _, srv := range updateSrvs {
		key := adapter.ServiceAddrAdapter.Encode(srv.uuid)
		value, err := json.Marshal(&ServiceResource{Addr: srv.addr})
		require.Nil(t, err)
		if srv.del {
			_, err = client.Delete(ctx, key)
			require.Nil(t, err)
		} else {
			_, err = client.Put(ctx, key, string(value))
			require.Nil(t, err)
		}
	}

	// test watch of service discovery
	ch := d.Watch(ctx)
	select {
	case wresp := <-ch:
		require.Nil(t, wresp.err)
		require.Equal(t, 2, len(wresp.addSet))
		require.Contains(t, wresp.addSet, "uuid-4")
		require.Contains(t, wresp.addSet, "uuid-5")
		require.Equal(t, 1, len(wresp.delSet))
		require.Contains(t, wresp.delSet, "uuid-1")
	case <-time.After(time.Second):
		require.Fail(t, "watch from service discovery timeout")
	}

	// test watch chan doesn't return when there is no change
	time.Sleep(2 * tickDur)
	select {
	case <-ch:
		require.Fail(t, "should not receive from channel when there is no change")
	default:
	}

	// test cancel will trigger watch to return an error
	cancel()
	wresp := <-ch
	require.Error(t, wresp.err, context.Canceled.Error())

	// test duplicate watch from service discovery
	ch = d.Watch(ctx)
	wresp = <-ch
	require.Error(t, wresp.err, errors.ErrDiscoveryDuplicateWatch.GetMsg())
}
