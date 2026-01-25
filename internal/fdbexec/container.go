package fdbexec

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/pkg/constants"
	"github.com/futura-platform/f4a/pkg/util"
	"github.com/futura-platform/futura/ftype/executiontype"
)

type ExecutionContainer struct {
	db        fdb.Database
	memoTable directory.DirectorySubspace
	callOrder directory.DirectorySubspace
}

var _ executiontype.TransactionalContainer = &ExecutionContainer{}

func NewContainer(id task.Id, db util.DbRoot) *ExecutionContainer {
	fdb.MustAPIVersion(constants.FDB_API_VERSION)

	tasks, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		panic(err)
	}
	tkey, err := tasks.TaskKey(db, id)
	if err != nil {
		panic(err)
	}
	memoTable, err := tkey.MemoTable(db)
	if err != nil {
		panic(err)
	}
	callOrder, err := tkey.CallOrder(db)
	if err != nil {
		panic(err)
	}
	return &ExecutionContainer{db: db.Database, memoTable: memoTable, callOrder: callOrder}
}

func (c *ExecutionContainer) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	_, err := c.db.Transact(func(t fdb.Transaction) (any, error) {
		err := fn(ctx, &executionTransaction{
			Transaction: t,
			executionReadTransaction: executionReadTransaction{
				ReadTransaction: t,
				memoTable:       c.memoTable,
				callOrder:       c.callOrder,
			},
		})
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *ExecutionContainer) ReadTransact(ctx context.Context, fn func(ctx context.Context, tx executiontype.ReadOnlyContainer) error) error {
	_, err := c.db.ReadTransact(func(t fdb.ReadTransaction) (any, error) {
		err := fn(ctx, &executionReadTransaction{
			ReadTransaction: t,
			memoTable:       c.memoTable,
			callOrder:       c.callOrder,
		})
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	return nil
}
