package srvdiscovery

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

// DiscoveryRunner defines an interface to run Discovery service
type DiscoveryRunner interface {
	ResetDiscovery(ctx context.Context, resetSession bool) (Session, error)
	GetWatcher() <-chan WatchResp
	// returns current snapshot, DiscoveryRunner maintains this as snapshot plus
	// revision, which can be used in any failover scenarios. The drawback is to
	// consume more memory, but since it contains node address information only,
	// the memory consumption is acceptable.
	GetSnapshot() Snapshot
	ApplyWatchResult(WatchResp)
}

type Snapshot map[UUID]ServiceResource

func (s Snapshot) Clone() Snapshot {
	snapshot := make(map[UUID]ServiceResource, len(s))
	for k, v := range s {
		snapshot[k] = v
	}
	return snapshot
}

// DiscoveryRunnerImpl implements DiscoveryRunner
type DiscoveryRunnerImpl struct {
	etcdCli    *clientv3.Client
	metastore  metadata.MetaKV
	sessionTTL int
	watchDur   time.Duration
	key        string
	value      string

	snapshot         Snapshot
	discovery        Discovery
	discoveryWatcher <-chan WatchResp
}

// NewDiscoveryRunnerImpl creates a new DiscoveryRunnerImpl
func NewDiscoveryRunnerImpl(
	etcdCli *clientv3.Client,
	metastore metadata.MetaKV,
	sessionTTL int,
	watchDur time.Duration,
	key string,
	value string,
) *DiscoveryRunnerImpl {
	return &DiscoveryRunnerImpl{
		etcdCli:    etcdCli,
		metastore:  metastore,
		sessionTTL: sessionTTL,
		watchDur:   watchDur,
		key:        key,
		value:      value,
	}
}

// Session defines a session used in discovery service
type Session interface {
	Done() <-chan struct{}
	Close() error
}

func (dr *DiscoveryRunnerImpl) connectToEtcdDiscovery(
	ctx context.Context, resetSession bool,
) (session Session, err error) {
	if resetSession {
		session, err = dr.createSession(ctx, dr.etcdCli)
		if err != nil {
			return
		}
	}

	// initialize a new service discovery, if old discovery exists, clones its
	// snapshot to the new one.
	old := dr.snapshot.Clone()
	dr.discovery = NewEtcdSrvDiscovery(
		dr.etcdCli, adapter.ExecutorInfoKeyAdapter, dr.watchDur)

	if len(old) > 0 {
		dr.discovery.CopySnapshot(old)
	} else {
		var snapshot Snapshot
		// must take a snapshot before discovery starts watch
		snapshot, err = dr.discovery.Snapshot(ctx)
		if err != nil {
			return
		}
		dr.snapshot = snapshot.Clone()
	}

	return
}

func (dr *DiscoveryRunnerImpl) createSession(ctx context.Context, etcdCli *clientv3.Client) (Session, error) {
	session, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(dr.sessionTTL))
	if err != nil {
		return nil, err
	}
	_, err = dr.metastore.Put(ctx, dr.key, dr.value, clientv3.WithLease(session.Lease()))
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (dr *DiscoveryRunnerImpl) ResetDiscovery(ctx context.Context, resetSession bool) (Session, error) {
	session, err := dr.connectToEtcdDiscovery(ctx, resetSession)
	if err != nil {
		return nil, err
	}
	dr.discovery.Close()
	dr.discoveryWatcher = dr.discovery.Watch(ctx)
	return session, nil
}

func (dr *DiscoveryRunnerImpl) GetSnapshot() Snapshot {
	return dr.snapshot
}

func (dr *DiscoveryRunnerImpl) GetWatcher() <-chan WatchResp {
	return dr.discoveryWatcher
}

func (dr *DiscoveryRunnerImpl) ApplyWatchResult(resp WatchResp) {
	for uuid, add := range resp.AddSet {
		dr.snapshot[uuid] = add
	}
	for uuid := range resp.DelSet {
		delete(dr.snapshot, uuid)
	}
}
