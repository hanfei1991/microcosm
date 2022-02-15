package metaclient

import (
	"context"
	"fmt"
	"testing"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/stretchr/testify/require"
)

type actType int

const (
	aNone actType = iota
	aGet
	aPut
	aDel
	aTxn
	aClose
)

type kv struct {
	key   string
	value string
}

type actUnit struct {
	a action
	e expect
	r result
}

type action struct {
	t  actType
	do kv
}

type expect struct {
	action
	err error
	res []kv
}

type result struct {
	// it may be different from the expect.err
	err error
}

func checkEqual(expected []kv, actual []*KeyValue) bool {
	// empty return
	if expected == nil && len(actual) == 0 {
		return true
	}

	if len(expected) != len(actual) {
		return false
	}

	for i, kv := range expected {
		act := actual[i]
		if act == nil {
			return false
		}

		if kv.key != string(act.Key) || kv.value != string(act.Value) {
			return false
		}
	}

	return true
}

func makeRows(kvs []kv) *Rows {
	if kvs == nil {
		return nil
	}

	rows := NewRows()
	for _, kv := range kvs {
		rows.AddRow(kv.key, kv.value)
	}

	return rows
}

func makeExpect(t *testing.T, mock KVClientMock, exp expect) {
	switch exp.t {
	case aGet:
		eg := mock.ExpectGet(exp.do.key)
		require.NotNil(t, eg)
		if exp.err != nil {
			eg.WillReturnError(exp.err)
		} else {
			eg.WillReturnResult(makeRows(exp.res))
		}
	case aPut:
		eg := mock.ExpectPut(exp.do.key, exp.do.value)
		require.NotNil(t, eg)
		if exp.err != nil {
			eg.WillReturnError(exp.err)
		}
	case aDel:
		eg := mock.ExpectDelete(exp.do.key)
		require.NotNil(t, eg)
		if exp.err != nil {
			eg.WillReturnError(exp.err)
		}
	case aClose:
		eg := mock.ExpectClose()
		require.NotNil(t, eg)
		if exp.err != nil {
			eg.WillReturnError(exp.err)
		}
	case aNone:
		// do nothing
	case aTxn:
		// [TODO]
	}
}

func testActions(t *testing.T, cli KVClient, mock KVClientMock, units []actUnit) {
	ctx := context.Background()
	for _, unit := range units {
		makeExpect(t, mock, unit.e)
		act := unit.a
		exp := unit.e
		res := unit.r
		switch act.t {
		case aGet:
			rsp, err := cli.Get(ctx, act.do.key)
			if unit.r.err != nil {
				require.Error(t, err)
				require.Regexp(t, res.err.Error(), err.Error())
				require.Nil(t, rsp)
			} else {
				require.Nil(t, err)
				require.True(t, checkEqual(exp.res, rsp.Kvs))
			}
		case aPut:
			rsp, err := cli.Put(ctx, act.do.key, act.do.value)
			if unit.r.err != nil {
				require.Error(t, err)
				require.Regexp(t, res.err.Error(), err.Error())
				require.Nil(t, rsp)
			} else {
				require.Nil(t, err)
			}
		case aDel:
			rsp, err := cli.Delete(ctx, act.do.key)
			if unit.r.err != nil {
				require.Error(t, err)
				require.Regexp(t, res.err.Error(), err.Error())
				require.Nil(t, rsp)
			} else {
				require.Nil(t, err)
			}
		case aClose:
			err := cli.Close()
			if unit.r.err != nil {
				require.Error(t, err)
				require.Regexp(t, res.err.Error(), err.Error())
			} else {
				require.Nil(t, err)
			}
		case aNone:
			// do nothing
		case aTxn:
			// [TODO]
		}
	}
}

func TestNormalMockKVClient(t *testing.T) {
	cli, mock := NewMockKVClient("127.0.0.1:123", "test")

	normalUnits := []actUnit{
		// normal get with expected result
		{
			a: action{
				t:  aGet,
				do: kv{key: "get"},
			},
			e: expect{
				action: action{
					t:  aGet,
					do: kv{key: "get", value: "world"},
				},
				res: []kv{
					{"hello", "world"},
					{"hello1", "world1"},
					{"hello2", "world2"},
				},
			},
			r: result{},
		},
		// normal get with expected empty result
		{
			a: action{
				t:  aGet,
				do: kv{key: "get1"},
			},
			e: expect{
				action: action{
					t:  aGet,
					do: kv{key: "get1", value: "world"},
				},
			},
			r: result{},
		},
		// normal get with expected error
		{
			a: action{
				t:  aGet,
				do: kv{key: "get2"},
			},
			e: expect{
				action: action{
					t:  aGet,
					do: kv{key: "get2", value: "world"},
				},
				err: cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Get"),
				res: []kv{
					{"hello", "world"},
					{"hello1", "world1"},
					{"hello2", "world2"},
				},
			},
			r: result{
				err: fmt.Errorf("inject error for Get.*"),
			},
		},
		// normal put with normal result
		{
			a: action{
				t:  aPut,
				do: kv{key: "put", value: "world"},
			},
			e: expect{
				action: action{
					t:  aPut,
					do: kv{key: "put", value: "world"},
				},
			},
		},
		// normal put with expected error
		{
			a: action{
				t:  aPut,
				do: kv{key: "put2", value: "world2"},
			},
			e: expect{
				action: action{
					t:  aPut,
					do: kv{key: "put2", value: "world2"},
				},
				err: cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Put"),
			},
			r: result{
				err: fmt.Errorf("inject error for Put*"),
			},
		},
		// normal delete with normal result
		{
			a: action{
				t:  aDel,
				do: kv{key: "delete", value: "world"},
			},
			e: expect{
				action: action{
					t:  aDel,
					do: kv{key: "delete", value: "world"},
				},
			},
		},
		// normal delete with expected error
		{
			a: action{
				t:  aDel,
				do: kv{key: "delete2", value: "world"},
			},
			e: expect{
				action: action{
					t:  aDel,
					do: kv{key: "delete2", value: "world"},
				},
				err: cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Delete"),
			},
			r: result{
				err: fmt.Errorf("inject error for Delete*"),
			},
		},
		// normal Close with normal result
		{
			a: action{
				t: aClose,
			},
			e: expect{
				action: action{
					t: aClose,
				},
			},
		},
		// normal Close with expected error
		{
			a: action{
				t: aClose,
			},
			e: expect{
				action: action{
					t: aClose,
				},
				err: cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Close"),
			},
			r: result{
				err: fmt.Errorf("inject error for Close*"),
			},
		},
	}

	testActions(t, cli, mock, normalUnits)
}

func TestAbnormalMockKVClient(t *testing.T) {
	cli, mock := NewMockKVClient("127.0.0.1:123", "test")

	abnormalUnits := []actUnit{
		// get with exceed expect
		{
			a: action{
				t:  aGet,
				do: kv{key: "get"},
			},
			e: expect{
				action: action{
					t: aNone,
				},
			},
			r: result{
				err: fmt.Errorf("exceed total expectation size*"),
			},
		},
		// get with wrong expectation
		{
			a: action{
				t:  aGet,
				do: kv{key: "get1"},
			},
			e: expect{
				action: action{
					t:  aPut,
					do: kv{key: "get1", value: "world"},
				},
			},
			r: result{
				err: fmt.Errorf("call to Get is not expected, next expectation*"),
			},
		},
		// consume last put expectation
		{
			a: action{
				t:  aPut,
				do: kv{key: "get1", value: "world"},
			},
			e: expect{
				action: action{
					t: aNone,
				},
			},
		},
		// get with unmatch expectation
		{
			a: action{
				t:  aGet,
				do: kv{key: "get2"},
			},
			e: expect{
				action: action{
					t:  aGet,
					do: kv{key: "get3", value: "world"},
				},
			},
			r: result{
				err: fmt.Errorf("Get expectation is unmatch, next expectation is*"),
			},
		},
		// consume last get expectation
		{
			a: action{
				t:  aGet,
				do: kv{key: "get3"},
			},
			e: expect{
				action: action{
					t: aNone,
				},
			},
		},
	}

	testActions(t, cli, mock, abnormalUnits)
}
