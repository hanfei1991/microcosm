package epoch

import (
	"context"
	"sync/atomic"

	"github.com/hanfei1991/microcosm/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	FakeKey   = "/fake-key"
	FakeValue = "/fake-value"
)

type Generator interface {
	GenerateEpoch(ctx context.Context) (int64, error)
}

func NewEpochGenerator(cli *clientv3.Client) Generator {
	return &epochGenerator{
		cli: cli,
	}
}

type epochGenerator struct {
	cli *clientv3.Client
}

// GenerateEpoch generate increasing epoch for job master epoch
func (e *epochGenerator) GenerateEpoch(ctx context.Context) (int64, error) {
	if e.cli == nil {
		return 0, errors.ErrMasterEtcdEpochFail.GenWithStack("invalid inner client for epoch generator")
	}
	resp, err := e.cli.Put(ctx, FakeKey, FakeValue)
	if err != nil {
		return 0, errors.Wrap(errors.ErrMasterEtcdEpochFail, err)
	}

	return resp.Header.Revision, nil
}

func NewMockEpochGenerator() Generator {
	return &mockEpochGenerator{}
}

type mockEpochGenerator struct {
	epoch int64
}

func (e *mockEpochGenerator) GenerateEpoch(ctx context.Context) (int64, error) {
	return atomic.AddInt64(&e.epoch, 1), nil
}
