package cluster

import (
	"context"
	"time"

	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type Election interface {
	Campaign(ctx context.Context, selfID NodeID) (leaderCtx context.Context, resign context.CancelFunc, err error)
}

type EtcdElectionConfig struct {
	CreateSessionTimeout time.Duration
	TTL                  time.Duration
	Prefix               EtcdKeyPrefix
}

type (
	EtcdKeyPrefix = string
	NodeID        = string
)

type EtcdElection struct {
	etcdClient *clientv3.Client
	election   *concurrency.Election
	session    *concurrency.Session
	rl         *rate.Limiter
}

func NewEtcdElection(
	ctx context.Context,
	etcdClient *clientv3.Client,
	session *concurrency.Session,
	config EtcdElectionConfig,
) (*EtcdElection, error) {
	ctx, cancel := context.WithTimeout(ctx, config.CreateSessionTimeout)
	defer cancel()

	var sess *concurrency.Session
	if session == nil {
		var err error
		sess, err = concurrency.NewSession(
			etcdClient,
			concurrency.WithContext(ctx),
			concurrency.WithTTL(int(config.TTL.Seconds())))
		if err != nil {
			return nil, derror.ErrMasterEtcdCreateSessionFail.Wrap(err).GenWithStackByArgs()
		}
	} else {
		sess = session
	}

	election := concurrency.NewElection(sess, config.Prefix)
	return &EtcdElection{
		etcdClient: etcdClient,
		election:   election,
		session:    sess,
		rl:         rate.NewLimiter(rate.Every(time.Second), 1 /* burst */),
	}, nil
}

func (e *EtcdElection) Campaign(ctx context.Context, selfID NodeID) (context.Context, context.CancelFunc, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(ctx.Err())
		default:
		}

		err := e.rl.Wait(ctx)
		if err != nil {
			// rl.Wait() can return an unnamed error `rate: Wait(n=%d) exceeds limiter's burst %d` if
			// ctx is canceled. This can be very confusing, so we must wrap it here.
			return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(err)
		}

		retCtx, resign, err := e.doCampaign(ctx, selfID)
		if err != nil {
			if errors.Cause(err) != mvcc.ErrCompacted {
				return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(err)
			}
			log.Warn("campaign for leader failed", zap.Error(err))
			continue
		}
		return retCtx, resign, nil
	}
}

func (e *EtcdElection) doCampaign(ctx context.Context, selfID NodeID) (context.Context, context.CancelFunc, error) {
	err := e.election.Campaign(ctx, selfID)
	if err != nil {
		return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(err)
	}
	retCtx := &sessionCtx{
		Context: ctx,
		sess:    e.session,
	}
	resignFn := func() {
		resignCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		err := e.election.Resign(resignCtx)
		if err != nil {
			log.Warn("resign leader failed", zap.Error(err))
		}
	}
	return retCtx, resignFn, nil
}

type sessionCtx struct {
	context.Context
	sess *concurrency.Session
}

func (c *sessionCtx) Done() <-chan struct{} {
	doneCh := make(chan struct{})
	go func() {
		select {
		case <-c.Context.Done():
		case <-c.sess.Done():
		}
		close(doneCh)
	}()
	return doneCh
}
