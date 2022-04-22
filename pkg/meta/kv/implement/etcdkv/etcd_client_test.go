package etcdkv

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/errors"
	metacom "github.com/hanfei1991/microcosm/pkg/meta/common"
	"github.com/hanfei1991/microcosm/pkg/meta/kv/kvclient"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"
)

type SuiteTestEtcd struct {
	// Include basic suite logic.
	suite.Suite
	e         *embed.Etcd
	endpoints string
}

func allocTempURL(t *testing.T) string {
	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

// The SetupSuite method will be run by testify once, at the very
// start of the testing suite, before any tests are run.
func (suite *SuiteTestEtcd) SetupSuite() {
	cfg := embed.NewConfig()
	tmpDir := "suite-etcd"
	dir, err := ioutil.TempDir("", tmpDir)
	require.Nil(suite.T(), err)
	cfg.Dir = dir
	peers := allocTempURL(suite.T())
	log.Printf("Allocate server peer port is %s", peers)
	u, err := url.Parse(peers)
	require.Nil(suite.T(), err)
	cfg.LPUrls = []url.URL{*u}
	advertises := allocTempURL(suite.T())
	log.Printf("Allocate server advertises port is %s", advertises)
	u, err = url.Parse(advertises)
	require.Nil(suite.T(), err)
	cfg.LCUrls = []url.URL{*u}
	suite.e, err = embed.StartEtcd(cfg)
	if err != nil {
		require.FailNow(suite.T(), "Start embedded etcd fail:%v", err)
	}
	select {
	case <-suite.e.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		suite.e.Server.Stop() // trigger a shutdown
		suite.e.Close()
		suite.e = nil
		require.FailNow(suite.T(), "Server took too long to start!")
	}
	suite.endpoints = advertises
}

// The TearDownSuite method will be run by testify once, at the very
// end of the testing suite, after all tests have been run.
func (suite *SuiteTestEtcd) TearDownSuite() {
	if suite.e != nil {
		suite.e.Server.Stop()
		suite.e.Close()
	}
}

func clearKeySpace(ctx context.Context, cli kvclient.KVClient) {
	cli.Delete(ctx, "", kvclient.WithFromKey())
}

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

type txnAction struct {
	acts []action
	// return error
	err error
}

func prepareData(ctx context.Context, t *testing.T, cli kvclient.KVClient, p prepare) {
	clearKeySpace(ctx, cli)
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

func testTxnAction(ctx context.Context, t *testing.T, cli kvclient.KVClient, txns []txnAction) {
	for _, txn := range txns {
		ops := make([]kvclient.Op, 0, len(txn.acts))
		for _, act := range txn.acts {
			switch act.t {
			case tGet:
				ops = append(ops, kvclient.OpGet(act.do.key, act.opts...))
			case tPut:
				ops = append(ops, kvclient.OpPut(act.do.key, act.do.value))
			case tDel:
				ops = append(ops, kvclient.OpDelete(act.do.key, act.opts...))
			default:
				require.FailNow(t, "unexpected action type")
			}
		}
		tx := cli.Txn(ctx)
		tx.Do(ops...)
		rsp, err := tx.Commit()
		// test txn rsp
		if txn.err != nil {
			require.Error(t, err)
			continue
		}
		require.Nil(t, err)
		require.Len(t, rsp.Responses, len(txn.acts))
		for i, r := range rsp.Responses {
			act := txn.acts[i]
			switch act.t {
			case tGet:
				rr := r.GetResponseGet()
				require.NotNil(t, rr)
				expected := act.q.expected
				require.Len(t, rr.Kvs, len(expected))
				for i, kv := range rr.Kvs {
					require.Equal(t, string(kv.Key), expected[i].key)
					require.Equal(t, string(kv.Value), expected[i].value)
				}
			case tPut:
				rr := r.GetResponsePut()
				require.NotNil(t, rr)
			case tDel:
				rr := r.GetResponseDelete()
				require.NotNil(t, rr)
			default:
				require.FailNow(t, "unexpected action type")
			}
		}
	}
}

func (suite *SuiteTestEtcd) TestBasicKV() {
	conf := &metacom.StoreConfigParams{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdImpl(conf)
	require.Nil(t, err)
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

func (suite *SuiteTestEtcd) TestKeyRangeOption() {
	conf := &metacom.StoreConfigParams{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdImpl(conf)
	require.Nil(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	input := prepare{
		kvs: []kv{
			{"hello1", "world1"},
			{"hello2", "world2"},
			{"interesting", "world"},
			{"dataflow", "engine"},
			{"TiDB", "component"},
		},
	}
	actions := []action{
		{
			t: tNone,
			q: query{
				key:  "hello",
				opts: []kvclient.OpOption{kvclient.WithRange("s")},
				expected: []kv{
					{"hello1", "world1"},
					{"hello2", "world2"},
					{"interesting", "world"},
				},
			},
		},
		{
			t: tNone,
			q: query{
				key:      "hello2",
				opts:     []kvclient.OpOption{kvclient.WithRange("Z")},
				expected: []kv{},
			},
		},
		{
			t: tNone,
			q: query{
				key:  "hello",
				opts: []kvclient.OpOption{kvclient.WithPrefix()},
				expected: []kv{
					{"hello1", "world1"},
					{"hello2", "world2"},
				},
			},
		},
		{
			t: tNone,
			q: query{
				key:  "Hello",
				opts: []kvclient.OpOption{kvclient.WithFromKey()},
				expected: []kv{
					{"TiDB", "component"},
					{"dataflow", "engine"},
					{"hello1", "world1"},
					{"hello2", "world2"},
					{"interesting", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"hello", ""},
			opts: []kvclient.OpOption{kvclient.WithPrefix()},
			q: query{
				key:  "",
				opts: []kvclient.OpOption{kvclient.WithFromKey()},
				expected: []kv{
					{"TiDB", "component"},
					{"dataflow", "engine"},
					{"interesting", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"AZ", ""},
			opts: []kvclient.OpOption{kvclient.WithRange("Titan")},
			q: query{
				key:  "",
				opts: []kvclient.OpOption{kvclient.WithFromKey()},
				expected: []kv{
					{"dataflow", "engine"},
					{"interesting", "world"},
				},
			},
		},
		{
			t:    tDel,
			do:   kv{"egg", ""},
			opts: []kvclient.OpOption{kvclient.WithFromKey()},
			q: query{
				key:  "",
				opts: []kvclient.OpOption{kvclient.WithFromKey()},
				expected: []kv{
					{"dataflow", "engine"},
				},
			},
		},
	}

	// test get key range(WithRange/WithPrefix/WithFromKey)
	prepareData(ctx, t, cli, input)
	testAction(ctx, t, cli, actions)

	cli.Close()
}

func (suite *SuiteTestEtcd) TestTxn() {
	conf := &metacom.StoreConfigParams{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdImpl(conf)
	require.Nil(t, err)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	input := prepare{
		kvs: []kv{
			{"hello1", "world1"},
			{"hello2", "world2"},
			{"interesting", "world"},
			{"dataflow", "engine"},
			{"TiDB", "component"},
		},
	}
	txns := []txnAction{
		{
			// etcd forbits same key op intersect(put/delete) in txn to avoid quadratic blowup??
			err: errors.ErrMetaOpFail, // [TODO] check the detail error
			acts: []action{
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []kvclient.OpOption{},
				},
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []kvclient.OpOption{kvclient.WithPrefix()},
				},
				{
					t:    tPut,
					do:   kv{"hello", "world"},
					opts: []kvclient.OpOption{},
				},
				{
					t:    tDel,
					do:   kv{"hello", ""},
					opts: []kvclient.OpOption{},
				},
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []kvclient.OpOption{kvclient.WithFromKey()},
				},
			},
		},
		{
			acts: []action{
				{
					t:    tGet,
					do:   kv{"hello", ""},
					opts: []kvclient.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tGet,
					do:   kv{"hell", ""},
					opts: []kvclient.OpOption{kvclient.WithPrefix()},
					q: query{
						expected: []kv{
							{"hello1", "world1"},
							{"hello2", "world2"},
						},
					},
				},
				{
					t:    tPut,
					do:   kv{"hello3", "world3"},
					opts: []kvclient.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tPut,
					do:   kv{"dataflow2", "engine2"},
					opts: []kvclient.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tDel,
					do:   kv{"dataflow3", ""},
					opts: []kvclient.OpOption{},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tDel,
					do:   kv{"int", ""},
					opts: []kvclient.OpOption{kvclient.WithPrefix()},
					q: query{
						expected: []kv{},
					},
				},
				{
					t:    tGet,
					do:   kv{"", ""},
					opts: []kvclient.OpOption{kvclient.WithFromKey()},
					q: query{
						expected: []kv{
							{"TiDB", "component"},
							{"dataflow", "engine"},
							{"dataflow2", "engine2"},
							{"hello1", "world1"},
							{"hello2", "world2"},
							{"hello3", "world3"},
						},
					},
				},
			},
		},
	}

	prepareData(ctx, t, cli, input)
	testTxnAction(ctx, t, cli, txns)

	cli.Close()
}

func (suite *SuiteTestEtcd) TestGenEpoch() {
	conf := &metacom.StoreConfigParams{
		Endpoints: []string{suite.endpoints},
	}
	t := suite.T()
	cli, err := NewEtcdImpl(conf)
	require.Nil(t, err)
	defer cli.Close()
	testGenerator(t, cli)
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

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestEtcdSuite(t *testing.T) {
	suite.Run(t, new(SuiteTestEtcd))
}
