package servicestate

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func taskSetRootPath() []string {
	return []string{"task_queue"}
}

func taskSetPath(runnerId string) []string {
	return append(taskSetRootPath(), runnerId)
}

func OpenTaskSetForRunner(tr fdb.ReadTransactor, db dbutil.DbRoot, runnerId string) (*RunnerSet, error) {
	return openRunnerSet(tr, db, runnerId)
}

func CreateOrOpenTaskSetForRunner(tr fdb.Transactor, db dbutil.DbRoot, runnerId string) (*RunnerSet, error) {
	return createOrOpenRunnerSet(tr, db, runnerId)
}

// ListTaskSets returns all associated runner ids for task sets in the database.
func ListTaskSets(rt fdb.ReadTransactor, db dbutil.DbRoot) ([]string, error) {
	l, err := db.Root.List(rt, taskSetRootPath())
	if err != nil {
		if errors.Is(err, directory.ErrDirNotExists) {
			// directory does not exist, so no task sets
			return nil, nil
		}
		return nil, err
	}
	return l, nil
}
