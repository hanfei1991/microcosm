package kv

import (
	"context"
	"time"

	metacom "github.com/hanfei1991/microcosm/pkg/meta/common"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/implement/etcdkv"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/namespace"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

// etcdKVClient is the implement of kv interface based on etcd
// Support namespace isolation and all kv ability
// etcdImpl -> kvPrefix+Closer -> etcdKVClient
type etcdKVClient struct {
	kvclient.Client
	kvclient.KV
	tenantID string
}

// NewKVClient return a kvclient without namespace
// [NOTICE]: this is for inner use currently
func NewKVClient(conf *metacom.StoreConfigParams) (kvclient.KVClientEx, error) {
	return etcdkv.NewEtcdImpl(conf)
}

// NewPrefixKVClient return a kvclient with namespace
func NewPrefixKVClient(cli kvclient.KVClientEx, tenantID string) kvclient.KVClient {
	pfKV := namespace.NewPrefixKV(cli, namespace.MakeNamespacePrefix(tenantID))
	return &etcdKVClient{
		Client:   cli,
		KV:       pfKV,
		tenantID: tenantID,
	}
}

// Ping check the connectivity of the specify metastore
// TODO: shall be an inner method of kvclient to check connectivity ??
func Ping(conf *metacom.StoreConfigParams) error {
	cliEx, err := NewKVClient(conf)
	if err != nil {
		return err
	}
	defer cliEx.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cli := NewPrefixKVClient(cliEx, tenant.TestTenantID)
	_, err = cli.Put(ctx, "test_key", "test_value")
	if err != nil {
		return err
	}

	return nil
}
