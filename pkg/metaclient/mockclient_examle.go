package metaclient

import (
	"context"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
)

// nolint:ineffassign, deadcode, unused
func usage() {
	cli, mock := NewMockKVClient("127.0.0.1:123", "testCluster")
	ctx := context.Background()

	// set expectation sequently
	mock.ExpectGet("Get").WillReturnResult(NewRows().AddRow("hello1", "world1").AddRow("hello2", "world2"))
	mock.ExpectPut("Put", "world")
	mock.ExpectDelete("Delete")
	expect := mock.ExpectTxn().WillDo(OpGet("Get")).WillDo(OpPut("Put", "world")).WillDo(OpDelete("Delete"))
	// size of txnRows need to equal to size of Op
	txnRows := NewTxnRows().AddRows(NewRows().AddRow("Get", "world").AddRow("Get2", "world"))
	txnRows.AddRows(NewRows()).AddRows(NewRows())
	expect.WillReturnResult(txnRows)
	mock.ExpectClose()

	// run operation
	getRsp, err := cli.Get(ctx, "Get")
	_ = getRsp
	// expect err == nil
	// expect rsp contains {"hello1", "world1"}, {"hello2", "world2"}
	putRsp, err := cli.Put(ctx, "Put", "world")
	_ = putRsp
	// expect err == nil
	delRsp, err := cli.Delete(ctx, "Delete", WithPrefix())
	_ = delRsp
	// expect err == nil
	txn := cli.Txn(ctx)
	txnRsp, err := txn.Do(OpGet("Get"), OpPut("Put", "world"), OpDelete("Delete")).Commit()
	_ = txnRsp
	// expect err == nil
	// expect rsp.Responses[0] contains {"Get", "world"}, {"Get2", "world"}
	err = cli.Close()
	_ = err
	// expect err == nil
}

// nolint:ineffassign, deadcode, unused
func useageDetail() {
	cli, mock := NewMockKVClient("127.0.0.1:123", "testCluster")
	ctx := context.Background()

	// Get with expected error
	mock.ExpectGet("Get").WillReturnError(cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Get"))
	rsp, err := cli.Get(ctx, "Get")
	// expect err == cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Get")

	// Get With expected result
	mock.ExpectGet("Get2").WillReturnResult(NewRows().AddRow("hello1", "world1").AddRow("hello2", "world2"))
	// we don't care the OpOption
	rsp, err = cli.Get(ctx, "Get2", WithRange("zz"))
	// expect err == nil
	// expect rsp contains {"hello1", "world1"}, {"hello2", "world2"}

	// Get with empty result
	mock.ExpectGet("Get3")
	// we don't care the OpOption
	rsp, err = cli.Get(ctx, "Get3", WithRange("zz"))
	// expect err == nil
	// expect rsp contains no records

	// Get with wrong expectation, unmatch Get key
	mock.ExpectGet("Get4")
	// we don't care the OpOption
	rsp, err = cli.Get(ctx, "Get4", WithRange("zz"))
	// expect err != nil
	// consume get expectation, continue the test
	rsp, err = cli.Get(ctx, "Get4", WithRange("zz"))

	// Get with wrong expectation, unmatch expectation
	mock.ExpectDelete("Get5")
	// we don't care the OpOption
	rsp, err = cli.Get(ctx, "Get5", WithRange("zz"))
	// expect err != nil
	// consume delete expectation, continue the test
	delRsp, err := cli.Delete(ctx, "Get5", WithRange("zz"))

	// Put with expected error
	mock.ExpectPut("Put", "world").WillReturnError(cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Put"))
	putRsp, err := cli.Put(ctx, "Put", "world")
	// expect err == cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Put")

	// Put with normal result
	mock.ExpectPut("Put2", "world")
	putRsp, err = cli.Put(ctx, "Put2", "world")
	// expect err == nil

	// Other Put error is the same as Get

	// Delete with expected error
	mock.ExpectDelete("Delete").WillReturnError(cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Delete"))
	delRsp, err = cli.Delete(ctx, "Delete")
	// expect err == cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Delete")

	// Delete with normal result
	mock.ExpectDelete("Delete")
	// we don't care the OpOption
	delRsp, err = cli.Delete(ctx, "Delete", WithPrefix())
	// expect err == nil

	// Other Delete error is the same as Get

	// Txn With expected error
	expect := mock.ExpectTxn().WillDo(OpGet("Get")).WillDo(OpPut("Put", "world")).WillDo(OpDelete("Delete"))
	expect.WillReturnError(cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Txn"))
	txn := cli.Txn(ctx)
	txnRsp, err := txn.Do(OpGet("Get"), OpPut("Put", "world"), OpDelete("Delete")).Commit()
	// expect err == cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Txn")

	// Txn With expected result
	expect = mock.ExpectTxn().WillDo(OpGet("Get")).WillDo(OpPut("Put", "world")).WillDo(OpDelete("Delete"))
	// size of txnRows need to equal to size of Op
	txnRows := NewTxnRows().AddRows(NewRows().AddRow("Get", "world").AddRow("Get2", "world"))
	txnRows.AddRows(NewRows()).AddRows(NewRows())
	expect.WillReturnResult(txnRows)
	txn = cli.Txn(ctx)
	txnRsp, err = txn.Do(OpGet("Get"), OpPut("Put", "world"), OpDelete("Delete")).Commit()
	// expect err == nil
	// expect rsp.Responses[0] contains {"Get", "world"}, {"Get2", "world"}

	// Other Txn error is the same as Get

	// Close with expected error
	mock.ExpectClose().WillReturnError(cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Close"))
	err = cli.Close()
	// expect err == cerrors.ErrMetaOpFail.GenWithStackByArgs("inject error for Close")

	// Close with normal result
	mock.ExpectClose()
	// we don't care the OpOption
	err = cli.Close()
	// expect err == nil

	// Other Close error is the same as Get

	_ = rsp
	_ = err
	_ = delRsp
	_ = putRsp
	_ = txnRsp
}
