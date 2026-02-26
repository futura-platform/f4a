package pool

import (
	"context"
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func taskSetRootPath() []string {
	return []string{"task_queue"}
}

func taskSetPath(runnerId string) []string {
	return append(taskSetRootPath(), runnerId)
}

func OpenTaskSetForRunner(db dbutil.DbRoot, runnerId string) (*reliableset.Set, context.CancelFunc, error) {
	return reliableset.Open(db, taskSetPath(runnerId))
}

func CreateOrOpenTaskSetForRunner(db dbutil.DbRoot, runnerId string) (*reliableset.Set, context.CancelFunc, error) {
	return reliableset.CreateOrOpen(db, taskSetPath(runnerId))
}

// ListTaskSets returns all associated runner ids for task sets in the database.
func ListTaskSets(tx fdb.ReadTransaction, db dbutil.DbRoot) ([]string, error) {
	l, err := db.Root.List(tx, taskSetRootPath())
	if err != nil {
		if errors.Is(err, directory.ErrDirNotExists) {
			// directory does not exist, so no task sets
			return nil, nil
		}
		return nil, err
	}
	return l, nil
}
