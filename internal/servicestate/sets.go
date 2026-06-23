package servicestate

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type taskIdSerializer struct{}

func (s taskIdSerializer) Marshal(id task.Id) []byte {
	return []byte(id)
}

func (s taskIdSerializer) Unmarshal(data []byte) (task.Id, error) {
	return task.Id(data), nil
}

func createOrOpenReadySet(tr fdb.Transactor, db dbutil.DbRoot) (reliableset.TSet[task.Id], error) {
	set, err := reliableset.CreateOrOpen(tr, db, []string{"ready"})
	if err != nil {
		return reliableset.TSet[task.Id]{}, err
	}
	return reliableset.MakeTSet(set, taskIdSerializer{}), nil
}

func createOrOpenSuspendedSet(tr fdb.Transactor, db dbutil.DbRoot) (reliableset.TSet[task.Id], error) {
	set, err := reliableset.CreateOrOpen(tr, db, []string{"suspended"})
	if err != nil {
		return reliableset.TSet[task.Id]{}, err
	}
	return reliableset.MakeTSet(set, taskIdSerializer{}), nil
}
