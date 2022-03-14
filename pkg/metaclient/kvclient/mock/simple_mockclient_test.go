package mock_metaclient

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/metaclient"
	"github.com/stretchr/testify/require"
)

type kv struct {
	key   string
	value string
}

type prepare struct {
	kvs []kv
}

type optype int

const (
	tNone optype = iota
	tGet
	tPut
	tDel
	tTxn
)

type query struct {
	key  string
	opts []metaclient.OpOption
	err  error
	// for txn: we only use expected
	expected []kv
}

type action struct {
	t optype
	// do action
	do   kv
	opts []metaclient.OpOption
	// query action
	q query
}

func prepareData(ctx context.Context, t *testing.T, cli metaclient.KVClient, p prepare) {
	if p.kvs != nil {
		for _, kv := range p.kvs {
			prsp, perr := cli.Put(ctx, kv.key, kv.value)
			require.Nil(t, perr)
			require.NotNil(t, prsp)
		}
	}
}

func testAction(ctx context.Context, t *testing.T, cli metaclient.KVClient, acts []action) {
	for _, act := range acts {
		switch act.t {
		case tGet:
			rsp, err := cli.Get(ctx, act.do.key, act.opts...)
			require.Nil(t, err)
			require.NotNil(t, rsp)
		case tPut:
			rsp, err := cli.Put(ctx, act.do.key, act.do.value)
			require.Nil(t, err)
			require.NotNil(t, rsp)
		case tDel:
			rsp, err := cli.Delete(ctx, act.do.key, act.opts...)
			require.Nil(t, err)
			require.NotNil(t, rsp)
		case tTxn:
			require.FailNow(t, "unexpected txn action")
		case tNone:
			// do nothing
		default:
			require.FailNow(t, "unexpected action type")
		}

		rsp, err := cli.Get(ctx, act.q.key, act.q.opts...)
		if act.q.err != nil {
			require.Error(t, err)
			continue
		}
		require.Nil(t, err)
		require.NotNil(t, rsp)
		expected := act.q.expected
		require.Len(t, rsp.Kvs, len(expected))
		for i, kv := range rsp.Kvs {
			require.Equal(t, string(kv.Key), expected[i].key)
			require.Equal(t, string(kv.Value), expected[i].value)
		}
	}
}

func TestMockBasicKV(t *testing.T) {
	cli := NewMetaMock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	input := prepare{
		kvs: []kv{},
	}
	actions := []action{
		{
			t: tNone,
			q: query{
				key:      "hello",
				opts:     []metaclient.OpOption{},
				expected: []kv{},
			},
		},
		{
			t:    tPut,
			do:   kv{"hello", "world"},
			opts: []metaclient.OpOption{},
			q: query{
				key:  "hello",
				opts: []metaclient.OpOption{},
				expected: []kv{
					{"hello", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"hello", ""},
			opts: []metaclient.OpOption{},
			q: query{
				key:      "hello",
				opts:     []metaclient.OpOption{},
				expected: []kv{},
			},
		},
		{
			t:    tPut,
			do:   kv{"hello", "new world"},
			opts: []metaclient.OpOption{},
			q: query{
				key:  "hello",
				opts: []metaclient.OpOption{},
				expected: []kv{
					{"hello", "new world"},
				},
			},
		},
	}

	// prepare data and test query
	prepareData(ctx, t, cli, input)
	testAction(ctx, t, cli, actions)

	cli.Close()
}
