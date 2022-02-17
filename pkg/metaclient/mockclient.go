package metaclient

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
)

type kvclientMock struct {
	sync.Mutex

	endpoint  string
	clusterID string

	// all expectations in mock
	expects []expectation
	// current expectation pos
	fulfilled int
}

// default size for expectation internal implement
const (
	DefaultExpectationSize = 10
	DefaultOpSize          = 10
	DefaultResultSize      = 10
)

// NewMockKVClient create a kvclient and kvclient mock
func NewMockKVClient(endpoint string, clusterID string) (KVClient, KVClientMock) {
	mock := &kvclientMock{
		endpoint:  endpoint,
		clusterID: clusterID,
		expects:   make([]expectation, 0, DefaultExpectationSize),
	}
	return mock, mock
}

// Implement KVClient interface

func (m *kvclientMock) Put(ctx context.Context, key string, value string) (*PutResponse, error) {
	m.Lock()
	defer m.Unlock()

	if m.fulfilled >= len(m.expects) {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("exceed total expectation size"))
	}

	exp, ok := m.expects[m.fulfilled].(*PutExpect)
	if !ok {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("call to Put is not expected, next expectation is %s", m.expects[m.fulfilled]))
	}

	if err := exp.check(key, value); err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("Put expectation is unmatch, next expectation is %s, %v", exp, err))
	}

	exp.fulfill()
	m.fulfilled++

	if exp.err != nil {
		return nil, exp.err
	}

	return &PutResponse{
		Header: &ResponseHeader{ClusterID: m.clusterID},
	}, nil
}

func (m *kvclientMock) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	m.Lock()
	defer m.Unlock()

	if m.fulfilled >= len(m.expects) {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("exceed total expectation size"))
	}

	exp, ok := m.expects[m.fulfilled].(*GetExpect)
	if !ok {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("call to Get is not expected, next expectation is %s", m.expects[m.fulfilled]))
	}

	if err := exp.check(key); err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("Get expectation is unmatch, next expectation is %s, %v", exp, err))
	}

	exp.fulfill()
	m.fulfilled++

	if exp.err != nil {
		return nil, exp.err
	}

	result := exp.result
	if result == nil {
		result = NewRows()
	}

	return &GetResponse{
		Header: &ResponseHeader{ClusterID: m.clusterID},
		Kvs:    result.rows,
	}, nil
}

func (m *kvclientMock) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	m.Lock()
	defer m.Unlock()

	if m.fulfilled >= len(m.expects) {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("exceed total expectation size"))
	}

	exp, ok := m.expects[m.fulfilled].(*DeleteExpect)
	if !ok {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("call to Delete is not expected, next expectation is %s", m.expects[m.fulfilled]))
	}

	if err := exp.check(key); err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("Delete expectation is unmatch, next expectation is %s, %v", exp, err))
	}

	exp.fulfill()
	m.fulfilled++

	if exp.err != nil {
		return nil, exp.err
	}

	return &DeleteResponse{
		Header: &ResponseHeader{ClusterID: m.clusterID},
	}, nil
}

// [TODO]
func (m *kvclientMock) Do(ctx context.Context, op Op) (OpResponse, error) {
	return OpResponse{}, nil
}

func (m *kvclientMock) Txn(ctx context.Context) Txn {
	return &mockTxn{
		m:   m,
		ops: make([]Op, 0, DefaultOpSize),
	}
}

func (m *kvclientMock) Close() error {
	m.Lock()
	defer m.Unlock()

	if m.fulfilled >= len(m.expects) {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("exceed total expectation size"))
	}

	exp, ok := m.expects[m.fulfilled].(*CloseExpect)
	if !ok {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("call to Close is not expected, next expectation is %s", m.expects[m.fulfilled]))
	}

	exp.fulfill()
	m.fulfilled++

	if exp.err != nil {
		return exp.err
	}
	return nil
}

type mockTxn struct {
	m   *kvclientMock
	ops []Op
}

func (txn *mockTxn) Do(ops ...Op) Txn {
	txn.ops = append(txn.ops, ops...)
	return txn
}

func (txn *mockTxn) Commit() (*TxnResponse, error) {
	txn.m.Lock()
	defer txn.m.Unlock()

	if txn.m.fulfilled >= len(txn.m.expects) {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("exceed total expectation size"))
	}

	exp, ok := txn.m.expects[txn.m.fulfilled].(*TxnExpect)
	if !ok {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("call to Txn is not expected, next expectation is %s", txn.m.expects[txn.m.fulfilled]))
	}
	if err := exp.check(txn.ops); err != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(fmt.Errorf("Txn expectation is unmatch, next expectation is %s, %v", exp, err))
	}

	exp.fulfill()
	txn.m.fulfilled++

	if exp.err != nil {
		return nil, exp.err
	}

	rsps := make([]ResponseOp, 0, len(txn.ops))
	for i, op := range txn.ops {
		var rsp ResponseOp
		switch {
		case op.IsGet():
			var r *Rows
			if exp.result == nil || exp.result.rowss[i] == nil {
				r = NewRows()
			} else {
				r = exp.result.rowss[i]
			}
			rsp = ResponseOp{
				Response: &ResponseOpResponseGet{
					ResponseGet: &GetResponse{
						Header: &ResponseHeader{ClusterID: txn.m.clusterID},
						Kvs:    r.rows,
					},
				},
			}
		case op.IsPut():
			rsp = ResponseOp{
				Response: &ResponseOpResponsePut{
					ResponsePut: &PutResponse{
						Header: &ResponseHeader{ClusterID: txn.m.clusterID},
					},
				},
			}
		case op.IsDelete():
			rsp = ResponseOp{
				Response: &ResponseOpResponseDelete{
					ResponseDelete: &DeleteResponse{
						Header: &ResponseHeader{ClusterID: txn.m.clusterID},
					},
				},
			}
		default:
			// [TODO]
		}

		rsps = append(rsps, rsp)
	}

	return &TxnResponse{
		Header:    &ResponseHeader{ClusterID: txn.m.clusterID},
		Responses: rsps,
	}, nil
}

// Construct result for `WillReturnResult`

// Rows is the result for a single Get response
// Use for GetExpect.WillReturnResult
type Rows struct {
	rows []*KeyValue
}

// NewRows return *Rows for GetExpect.WillReturnResult
func NewRows() *Rows {
	return &Rows{
		rows: make([]*KeyValue, 0, DefaultResultSize),
	}
}

// AddRow will add a single row in Rows
func (r *Rows) AddRow(key string, value string) *Rows {
	r.rows = append(r.rows, &KeyValue{Key: []byte(key), Value: []byte(value)})
	return r
}

// AddRow will add variable rows in Rows
func (r *Rows) AddRows(kvs ...*KeyValue) *Rows {
	r.rows = append(r.rows, kvs...)
	return r
}

func (r *Rows) String() string {
	msg := "["
	for _, row := range r.rows {
		msg += fmt.Sprintf("{%s, %s},", string(row.Key), string(row.Value))
	}
	msg += "]"

	return msg
}

// TxnRows is the result for a single Txn.Commit() response
// Use for TxnExpect.WillReturnResult
type TxnRows struct {
	rowss []*Rows
}

func NewTxnRows() *TxnRows {
	return &TxnRows{
		rowss: make([]*Rows, 0, DefaultResultSize),
	}
}

func (t *TxnRows) AddRows(rows ...*Rows) *TxnRows {
	t.rowss = append(t.rowss, rows...)
	return t
}

func (t *TxnRows) String() string {
	msg := "["
	for _, rows := range t.rowss {
		msg += fmt.Sprintf("[%s],", rows.String())
	}
	msg += "]"

	return msg
}

// an expectation interface
type expectation interface {
	fulfilled() bool
	fulfill()
	String() string
}

// common expectation struct
// satisfies the expectation interface
type commonExpectation struct {
	triggered bool
	err       error // nolint:structcheck
}

// fulfilled return whether this expectation has been fulfilled
func (e *commonExpectation) fulfilled() bool {
	return e.triggered
}

// fulfill set this expectation as fulfilled
func (e *commonExpectation) fulfill() {
	e.triggered = true
}

// DeleteExpect is the expectation for ExpectDelete
type DeleteExpect struct {
	commonExpectation
	key string
}

// WillReturnError will return an error when user call Delete
func (e *DeleteExpect) WillReturnError(err error) *DeleteExpect {
	e.err = err
	return e
}

// String returns string representation
func (e *DeleteExpect) String() string {
	msg := fmt.Sprintf("ExpectDelete => expecting kvclient delete key:%s", e.key)
	if e.err != nil {
		msg += fmt.Sprintf(", which should return error: %s", e.err)
	}
	return msg
}

// check check whether the user's Delete call is the same as DeleteExpect
func (e *DeleteExpect) check(key string) error {
	if e.key == key {
		return nil
	}

	return fmt.Errorf("Delete not equal, expect key:%s, actual key:%s", e.key, key)
}

// PutExpect is the expectation for ExpectPut
type PutExpect struct {
	commonExpectation
	key   string
	value string
}

// WillReturnError will return an error when user call Put
func (e *PutExpect) WillReturnError(err error) *PutExpect {
	e.err = err
	return e
}

// String returns string representation
func (e *PutExpect) String() string {
	msg := fmt.Sprintf("ExpectPut => expecting kvclient put:{%s, %s}", e.key, e.value)
	if e.err != nil {
		msg += fmt.Sprintf(", which should return error: %s", e.err)
	}
	return msg
}

// check check whether the user's Put call is the same as PutExpect
func (e *PutExpect) check(key, value string) error {
	if e.key == key && e.value == value {
		return nil
	}

	return fmt.Errorf("Put not equal, expect kv:{%s, %s}, actual:{%s, %s}", e.key, e.value, key, value)
}

// GetExpect is the expectation for ExpectGet
type GetExpect struct {
	commonExpectation
	key    string
	result *Rows
}

// WillReturnError will return an error when user call Get
func (e *GetExpect) WillReturnError(err error) *GetExpect {
	e.err = err
	return e
}

// WillReturnResult will return a result when user call Get
func (e *GetExpect) WillReturnResult(rows *Rows) *GetExpect {
	e.result = rows
	return e
}

// String returns string representation
func (e *GetExpect) String() string {
	msg := fmt.Sprintf("ExpectGet => expecting kvclient get:%s, ", e.key)
	if e.err != nil {
		msg += fmt.Sprintf(", which should return error: %s", e.err)
	}

	msg += fmt.Sprintf(", which should return result:%s", e.result)
	return msg
}

// check check whether the user's Get call is the same as GetExpect
func (e *GetExpect) check(key string) error {
	if e.key == key {
		return nil
	}

	return fmt.Errorf("Get not equal, expect key:%s, actual key:%s", e.key, key)
}

// CloseExpect is the expectation for ExpectClose
type CloseExpect struct {
	commonExpectation
}

// WillReturnError will return an error when user call Close
func (e *CloseExpect) WillReturnError(err error) *CloseExpect {
	e.err = err
	return e
}

// String returns string representation
func (e *CloseExpect) String() string {
	msg := "ExpectClose => expecting kvclient close"
	if e.err != nil {
		msg += fmt.Sprintf(", which should return error: %s", e.err)
	}
	return msg
}

// TxnExpect is the expectation for ExpectTxn
type TxnExpect struct {
	commonExpectation
	ops    []Op
	result *TxnRows
}

// WillDo set the expected Ops for txn
func (e *TxnExpect) WillDo(p ...Op) *TxnExpect {
	if e.ops == nil {
		e.ops = make([]Op, 0, len(p))
	}
	e.ops = append(e.ops, p...)
	return e
}

// WillReturnError will return an error when user call Txn.Commit()
func (e *TxnExpect) WillReturnError(err error) *TxnExpect {
	e.err = err
	return e
}

// WillReturnResult will return an error when user call Txn.Commit()
func (e *TxnExpect) WillReturnResult(txnRows *TxnRows) *TxnExpect {
	e.result = txnRows
	return e
}

// String returns string representation
func (e *TxnExpect) String() string {
	msg := "ExpectTxn => expecting kvclient txn"
	if e.err != nil {
		msg += fmt.Sprintf(", which should return error: %s", e.err)
	}

	ost := "["
	for _, op := range e.ops {
		ost += fmt.Sprintf("{t:%d, key:%s, value:%s},", op.T, op.KeyBytes(), op.ValueBytes())
	}
	ost += "]"

	msg += fmt.Sprintf(", which should with ops:%s and result:%s", ost, e.result)
	return msg
}

// check check whether the user's Txn operation is the same as TxnExpect
func (e *TxnExpect) check(ops []Op) error {
	if len(e.ops) != len(ops) || (len(e.ops) != 0 && e.result != nil && len(e.result.rowss) != len(e.ops)) {
		return fmt.Errorf("expect ops size is not equal with actual ops size or the result size")
	}

	for i, op := range e.ops {
		rop := ops[i]

		if op.T != rop.T {
			return fmt.Errorf("The %dth Op type is not equal, expect:%d, actual:%d", i, int(op.T), int(rop.T))
		}

		if 0 != bytes.Compare(op.KeyBytes(), rop.KeyBytes()) {
			return fmt.Errorf("The %dth Op key is not equal, expect:%s, actual:%s", i, string(op.KeyBytes()), string(rop.KeyBytes()))
		}

		if op.IsPut() && 0 != bytes.Compare(op.ValueBytes(), rop.ValueBytes()) {
			return fmt.Errorf("The %dth Put Op value is not equal, expect:%s, actual:%s", i, string(op.ValueBytes()), string(rop.ValueBytes()))
		}
	}

	return nil
}

// KVClientMock is the kvclient mock interface
type KVClientMock interface {
	// ExpectPut set the Put expectation with key and value, return *PutExpect
	// User can use `WillReturnError` to set and return an expected error
	// Or return a normal response when call Put()
	ExpectPut(key, value string) *PutExpect

	// ExpectGet set the Get expectation with key, return *GetExpect
	// Currently, we only check key and ignore OpOption
	// User can use `WillReturnError` to set and return an expected error
	// Or use `WillReturnResult` to set and return an expected result
	// Or return a normal result without any record when call Get()
	ExpectGet(key string) *GetExpect

	// ExpectDelete set the Delete expectation with key, return *DeleteExpect
	// User can use `WillReturnError` to set and return an expected error
	// Or return a normal response when call Delete()
	ExpectDelete(key string) *DeleteExpect

	// ExpectTxn set the Txn expectation, return *TxnExpect
	// User can use `WillDo` to set some expected Op
	// User can use `WillReturnError` to set return an expected error
	// Or use `WillReturnResult` to set and return an expected result
	// Or return a normal response when call Txn.Commit()
	ExpectTxn() *TxnExpect

	// ExpectClose set the Close expectation, return *CloseExpect
	// User can use `WillReturnError` to set and return an expected error
	// Or return nil when call Close()
	ExpectClose() *CloseExpect
}

// Implement KVClientMock inteface

func (m *kvclientMock) ExpectPut(key, value string) *PutExpect {
	e := &PutExpect{
		key:   key,
		value: value,
	}
	m.expects = append(m.expects, e)
	return e
}

func (m *kvclientMock) ExpectGet(key string) *GetExpect {
	e := &GetExpect{
		key: key,
	}
	m.expects = append(m.expects, e)
	return e
}

func (m *kvclientMock) ExpectDelete(key string) *DeleteExpect {
	e := &DeleteExpect{
		key: key,
	}
	m.expects = append(m.expects, e)
	return e
}

func (m *kvclientMock) ExpectTxn() *TxnExpect {
	e := &TxnExpect{
		ops: make([]Op, 0, DefaultOpSize),
	}
	m.expects = append(m.expects, e)
	return e
}

func (m *kvclientMock) ExpectClose() *CloseExpect {
	e := &CloseExpect{}
	m.expects = append(m.expects, e)
	return e
}
