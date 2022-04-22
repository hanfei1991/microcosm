package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
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
	opts []kvclient.OpOption
	err  error
	// for txn: we only use expected
	expected []kv
}

type action struct {
	t optype
	// do action
	do   kv
	opts []kvclient.OpOption
	// query action
	q query
}

func prepareData(ctx context.Context, t *testing.T, cli kvclient.KVClient, p prepare) {
	if p.kvs != nil {
		for _, kv := range p.kvs {
			prsp, perr := cli.Put(ctx, kv.key, kv.value)
			require.Nil(t, perr)
			require.NotNil(t, prsp)
		}
	}
}

func testAction(ctx context.Context, t *testing.T, cli kvclient.KVClient, acts []action) {
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
	t.Parallel()

	cli := NewMetaMock()
	defer cli.Close()
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
				opts:     []kvclient.OpOption{},
				expected: []kv{},
			},
		},
		{
			t:    tPut,
			do:   kv{"hello", "world"},
			opts: []kvclient.OpOption{},
			q: query{
				key:  "hello",
				opts: []kvclient.OpOption{},
				expected: []kv{
					{"hello", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"hello", ""},
			opts: []kvclient.OpOption{},
			q: query{
				key:      "hello",
				opts:     []kvclient.OpOption{},
				expected: []kv{},
			},
		},
		{
			t:    tPut,
			do:   kv{"hello", "new world"},
			opts: []kvclient.OpOption{},
			q: query{
				key:  "hello",
				opts: []kvclient.OpOption{},
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

func testGenerator(t *testing.T, kvcli kvclient.KVClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	firstEpoch, err := kvcli.GenEpoch(ctx)
	require.Nil(t, err)
	require.GreaterOrEqual(t, firstEpoch, int64(0))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			epoch, err := kvcli.GenEpoch(ctx)
			require.Nil(t, err)
			require.GreaterOrEqual(t, epoch, int64(0))
			oldEpoch := epoch

			epoch, err = kvcli.GenEpoch(ctx)
			require.Nil(t, err)
			require.Greater(t, epoch, oldEpoch)
		}()
	}

	wg.Wait()
	lastEpoch, err := kvcli.GenEpoch(ctx)
	require.Nil(t, err)
	require.Equal(t, int64(201), lastEpoch-firstEpoch)
}

func TestGenEpoch(t *testing.T) {
	t.Parallel()

	cli := NewMetaMock()
	defer cli.Close()
	testGenerator(t, cli)
}

func TestMockTxn(t *testing.T) {
	t.Parallel()

	cli := NewMetaMock()
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// prepare data
	_, err := cli.Put(ctx, "key1", "value1")
	require.Nil(t, err)
	_, err = cli.Put(ctx, "key2", "value2")
	require.Nil(t, err)

	txn := cli.Txn(ctx)
	txn.Do(kvclient.OpGet("key1"))
	txn.Do(kvclient.OpPut("key3", "value3"))
	txn.Do(kvclient.OpDelete("key2"))
	txn.Do(kvclient.OpGet("key2"))
	txnRsp, err := txn.Commit()
	require.Nil(t, err)
	require.Len(t, txnRsp.Responses, 4)

	getRsp := txnRsp.Responses[0].GetResponseGet()
	require.Len(t, getRsp.Kvs, 1)
	require.Equal(t, "key1", string(getRsp.Kvs[0].Key))
	require.Equal(t, "value1", string(getRsp.Kvs[0].Value))

	putRsp := txnRsp.Responses[1].GetResponsePut()
	require.NotNil(t, putRsp)

	delRsp := txnRsp.Responses[2].GetResponseDelete()
	require.NotNil(t, delRsp)

	getRsp = txnRsp.Responses[3].GetResponseGet()
	require.Len(t, getRsp.Kvs, 0)

	rsp, err := cli.Txn(ctx).Do(kvclient.OpTxn([]kvclient.Op{kvclient.EmptyOp})).Commit()
	require.Nil(t, rsp)
	require.Error(t, err)

	rsp, err = cli.Txn(ctx).Do(kvclient.EmptyOp).Commit()
	require.Nil(t, rsp)
	require.Error(t, err)
}
