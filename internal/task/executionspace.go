package task

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

func (k TaskKey) MemoTable(db fdb.Transactor) (directory.DirectorySubspace, error) {
	return k.d.CreateOrOpen(db, []string{"memo_table"}, nil)
}

func (k TaskKey) CallOrder(db fdb.Transactor) (directory.DirectorySubspace, error) {
	return k.d.CreateOrOpen(db, []string{"call_order"}, nil)
}
