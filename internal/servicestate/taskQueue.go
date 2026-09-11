package servicestate

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"golang.org/x/sync/errgroup"
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

const cardinalityReadParallelism = 16

func CountTasksForRunners(t fdb.ReadTransaction, db dbutil.DbRoot, runnerIds []string) (int64, error) {
	counts := make([]int64, len(runnerIds))
	var reads errgroup.Group
	reads.SetLimit(cardinalityReadParallelism)
	for i, runnerId := range runnerIds {
		reads.Go(func() error {
			value, err := reliableset.ReadCardinality(t, db, taskSetPath(runnerId))
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					return nil
				}
				return err
			}
			counts[i] = value
			return nil
		})
	}
	if err := reads.Wait(); err != nil {
		return 0, err
	}
	var count int64
	for _, value := range counts {
		count += value
	}
	return count, nil
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
