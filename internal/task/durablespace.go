package task

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

func (k TaskKey) DurableObjectSpace(db fdb.Transactor) (directory.DirectorySubspace, error) {
	return k.d.CreateOrOpen(db, []string{"durable"}, nil)
}
