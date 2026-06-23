package servicestate

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliableset"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// TaskPlacer is responsible for placing tasks in the system.
// It maintains task lifecycle status, the task runner id field, global utilization aggregates.
type TaskPlacer struct {
	db dbutil.DbRoot

	// a queue of tasks that are ready to be executed.
	pendingSet,
	// a queue of tasks that are suspended (have state, but are not executing).
	suspendedSet *reliableset.Set

	// a global utilization aggregate that tracks the total utilization of all placed tasks in the system.
	globalUtilization *utilizationAggregate
}

func CreateOrOpenTaskPlacer(db dbutil.DbRoot) (
	placer *TaskPlacer,
	runCompactor func() (cancel func()),
	err error,
) {
	pendingSet, err := createOrOpenReadySet(db, db)
	if err != nil {
		return nil, nil, err
	}
	suspendedSet, err := createOrOpenSuspendedSet(db, db)
	if err != nil {
		return nil, nil, err
	}
	globalUtilization, err := createOrOpenUtilizationAggregate(db, db, []string{"global_utilization"})
	if err != nil {
		return nil, nil, err
	}
	return &TaskPlacer{
			db:                db,
			pendingSet:        pendingSet,
			suspendedSet:      suspendedSet,
			globalUtilization: globalUtilization,
		}, func() (cancel func()) {
			cancelPendingSetCompaction := pendingSet.RunCompactor()
			cancelSuspendedSetCompaction := suspendedSet.RunCompactor()
			return func() {
				cancelPendingSetCompaction()
				cancelSuspendedSetCompaction()
			}
		}, nil
}

func (p TaskPlacer) GetGlobalUtilization(tx fdb.ReadTransaction, dimension UtilizationDimension) (int64, error) {
	return p.globalUtilization.get(tx, dimension)
}

func (p TaskPlacer) StreamPendingTasks(ctx context.Context) (initialValues mapset.Set[string], events <-chan []reliableset.LogEntry, errCh <-chan error, err error) {
	return p.pendingSet.Stream(ctx)
}

func (p TaskPlacer) PendingTasks(ctx context.Context) (taskIds mapset.Set[string], tail fdb.KeyConvertible, err error) {
	return p.pendingSet.Items(ctx, p.db.Database)
}

func (p TaskPlacer) SuspendedTasks(ctx context.Context) (taskIds mapset.Set[string], tail fdb.KeyConvertible, err error) {
	return p.suspendedSet.Items(ctx, p.db.Database)
}
