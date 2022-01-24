package servermaster

import (
	"context"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

func (s *Server) leaderLoop(ctx context.Context) error {
	retryInterval := time.Millisecond * 200
	for {
		select {
		case <-ctx.Done():
			return perrors.Trace(ctx.Err())
		default:
		}
		key, data, rev, err := etcdutils.GetLeader(ctx, s.etcdClient, adapter.MasterCampaignKey.Path())
		if err != nil {
			if perrors.Cause(err) == context.Canceled {
				return nil
			}
			if !errors.ErrMasterNoLeader.Equal(err) {
				log.L().Warn("get leader failed", zap.Error(err))
				time.Sleep(retryInterval)
				continue
			}
		}
		var leader *Member
		if len(data) > 0 {
			leader = &Member{}
			err = leader.Unmarshal(data)
			if err != nil {
				log.L().Warn("unexpected leader data", zap.Error(err))
				time.Sleep(retryInterval)
				continue
			}
			if leader.Name == s.name() {
				// this server is already a leader, which indicates there is some
				// stale information, just delete the leadership and campaign again.
				log.L().Warn("found stale leader key, delete it and campaign later",
					zap.ByteString("key", key), zap.ByteString("val", data))
				_, err := s.etcdClient.Delete(ctx, string(key))
				if err != nil {
					log.L().Error("failed to delete leader key",
						zap.ByteString("key", key), zap.ByteString("val", data))
				}
				time.Sleep(retryInterval)
				continue
			}
			leader.IsServLeader = true
			leader.IsEtcdLeader = true
		}
		if leader != nil {
			log.L().Info("start to watch server master leader",
				zap.String("leader-name", leader.Name), zap.String("addr", leader.AdvertiseAddr))
			s.watchLeader(ctx, leader, rev)
			log.L().Info("server master leader changed")
		}
		if !s.isEtcdLeader() {
			log.L().Info("skip campaigning leader", zap.String("name", s.name()))
			time.Sleep(retryInterval)
			continue
		}
		err = s.campaign(ctx, defaultCampaignTimeout)
		if err != nil {
			continue
		}

		err = s.leaderServiceFn(s.leaderCtx)
		if err != nil {
			if perrors.Cause(err) == context.Canceled ||
				errors.ErrEtcdLeaderChanged.Equal(err) {
				log.L().Info("leader service exits", zap.Error(err))
			} else if errors.ErrMasterSessionDone.Equal(err) {
				log.L().Info("server master session done, reset session now", zap.Error(err))
				err2 := s.reset(ctx)
				if err2 != nil {
					return err2
				}
			} else {
				log.L().Error("run leader service failed", zap.Error(err))
			}
		}
	}
}

func (s *Server) resign() {
	s.resignFn()
}

func (s *Server) campaign(ctx context.Context, timeout time.Duration) error {
	log.L().Info("start to campaign server master leader", zap.String("name", s.name()))
	leaderCtx, resignFn, err := s.election.Campaign(ctx, s.member(), timeout)
	switch perrors.Cause(err) {
	case nil:
	case context.Canceled:
		return ctx.Err()
	default:
		log.L().Warn("campaign leader failed", zap.Error(err))
		return errors.Wrap(errors.ErrMasterCampaignLeader, err)
	}
	s.leaderCtx = leaderCtx
	s.resignFn = resignFn
	log.L().Info("campaign leader successfully", zap.String("name", s.name()))
	return nil
}

func (s *Server) createLeaderClient(ctx context.Context, addrs []string) {
	s.closeLeaderClient()

	endpoints := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		endpoints = append(endpoints, strings.Replace(addr, "http://", "", 1))
	}
	cli, err := client.NewMasterClient(ctx, endpoints)
	if err != nil {
		log.L().Error("create server master client failed", zap.Strings("addrs", addrs), zap.Error(err))
		return
	}
	s.leaderClient.Lock()
	s.leaderClient.cli = cli
	s.leaderClient.Unlock()
}

func (s *Server) closeLeaderClient() {
	s.leaderClient.Lock()
	defer s.leaderClient.Unlock()
	if s.leaderClient.cli != nil {
		err := s.leaderClient.cli.Close()
		if err != nil {
			log.L().Warn("close leader client met error", zap.Error(err))
		}
		s.leaderClient.cli = nil
	}
}

func (s *Server) isEtcdLeader() bool {
	return s.etcd.Server.Lead() == uint64(s.etcd.Server.ID())
}

func (s *Server) watchLeader(ctx context.Context, m *Member, rev int64) {
	m.IsServLeader = true
	m.IsEtcdLeader = true
	s.leader.Store(m)
	s.createLeaderClient(ctx, []string{m.AdvertiseAddr})
	defer s.leader.Store(&Member{})

	watcher := clientv3.NewWatcher(s.etcdClient)
	defer watcher.Close()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		leaderPath := adapter.MasterCampaignKey.Path()
		rch := watcher.Watch(ctx, leaderPath, clientv3.WithPrefix(), clientv3.WithRev(rev))
		for wresp := range rch {
			if wresp.CompactRevision != 0 {
				log.L().Warn("watch leader met compacted error",
					zap.Int64("required-revision", rev),
					zap.Int64("compact-revision", wresp.CompactRevision),
				)
				rev = wresp.CompactRevision
				break
			}
			if wresp.Canceled {
				log.L().Warn("leadership watcher is canceled",
					zap.Int64("revision", rev), zap.String("leader-name", m.Name),
					zap.Error(wresp.Err()))
				return
			}
			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.L().Info("current leader is resigned", zap.String("leader-name", m.Name))
					return
				}
			}
		}
	}
}
