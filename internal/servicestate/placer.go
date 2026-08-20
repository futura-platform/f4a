package servicestate

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// TaskPlacer is responsible for placing tasks in the system.
// It maintains task lifecycle status, the task runner id field, and utilization aggregates.
type TaskPlacer struct {
	db dbutil.DbRoot

	// a queue of tasks that are ready to be executed.
	pendingSet,
	// a queue of tasks that are suspended (have state, but are not executing).
	suspendedSet reliableset.TSet[task.Id]

	// utilization requested by tasks that should influence runner capacity.
	activeDemandUtilization *utilizationAggregate
	// utilization requested by suspended tasks.
	suspendedUtilization *utilizationAggregate
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
	activeDemandUtilization, err := createOrOpenUtilizationAggregate(db, db, []string{"active_demand_utilization"})
	if err != nil {
		return nil, nil, err
	}
	suspendedUtilization, err := createOrOpenUtilizationAggregate(db, db, []string{"suspended_utilization"})
	if err != nil {
		return nil, nil, err
	}
	return &TaskPlacer{
			db:                      db,
			pendingSet:              pendingSet,
			suspendedSet:            suspendedSet,
			activeDemandUtilization: activeDemandUtilization,
			suspendedUtilization:    suspendedUtilization,
		}, func() (cancel func()) {
			cancelPendingSetCompaction := pendingSet.RunCompactor()
			cancelSuspendedSetCompaction := suspendedSet.RunCompactor()
			return func() {
				cancelPendingSetCompaction()
				cancelSuspendedSetCompaction()
			}
		}, nil
}

func (p TaskPlacer) GetActiveDemandUtilization(tx fdb.ReadTransaction, dimension UtilizationDimension) (int64, error) {
	return p.activeDemandUtilization.get(tx, dimension)
}

func (p TaskPlacer) GetSuspendedUtilization(tx fdb.ReadTransaction, dimension UtilizationDimension) (int64, error) {
	return p.suspendedUtilization.get(tx, dimension)
}

func (p TaskPlacer) StreamPendingTasks(ctx context.Context) (initialValues mapset.Set[task.Id], events <-chan []reliableset.TLogEntry[task.Id], errCh <-chan error, err error) {
	return p.pendingSet.Stream(ctx)
}

func (p TaskPlacer) PendingTasks(ctx context.Context) (taskIds mapset.Set[task.Id], tail fdb.KeyConvertible, err error) {
	return p.pendingSet.Items(ctx, p.db.Database)
}

// PendingTaskCount returns the pending set's cardinality as of its last
// compaction (eventually consistent with PendingTasks), as a single-key read.
func (p TaskPlacer) PendingTaskCount(t fdb.ReadTransaction) (int64, error) {
	return p.pendingSet.Cardinality(t)
}

// SuspendedTaskCount returns the suspended set's cardinality as of its last
// compaction (eventually consistent with SuspendedTasks), as a single-key read.
func (p TaskPlacer) SuspendedTaskCount(t fdb.ReadTransaction) (int64, error) {
	return p.suspendedSet.Cardinality(t)
}

func (p TaskPlacer) SuspendedTasks(ctx context.Context) (taskIds mapset.Set[task.Id], tail fdb.KeyConvertible, err error) {
	return p.suspendedSet.Items(ctx, p.db.Database)
}
