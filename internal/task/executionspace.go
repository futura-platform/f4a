package task

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

func (k TaskKey) MemoTable(db fdb.Transactor, namespace string) (directory.DirectorySubspace, error) {
	return k.d.CreateOrOpen(db, []string{namespace, "memo_table"}, nil)
}

func (k TaskKey) CallOrder(db fdb.Transactor, namespace string) (directory.DirectorySubspace, error) {
	return k.d.CreateOrOpen(db, []string{namespace, "call_order"}, nil)
}
