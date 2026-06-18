package pool

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// RunnerSet is a helper wrapper around a set that ensures the utilization aggregate is updated whenever the set is mutated.
type RunnerSet struct {
	*reliableset.Set
	utilizationAggregate *utilizationAggregate
}

func openRunnerSet(tr fdb.ReadTransactor, db dbutil.DbRoot, runnerId string) (*RunnerSet, error) {
	set, err := reliableset.Open(tr, db, taskSetPath(runnerId))
	if err != nil {
		return nil, err
	}
	utilizationAggregate, err := openUtilizationAggregate(tr, db, append(taskSetPath(runnerId), "utilization_aggregate"))
	if err != nil {
		return nil, err
	}
	return &RunnerSet{set, utilizationAggregate}, nil
}

func createOrOpenRunnerSet(tr fdb.Transactor, db dbutil.DbRoot, runnerId string) (*RunnerSet, error) {
	set, err := reliableset.CreateOrOpen(tr, db, taskSetPath(runnerId))
	if err != nil {
		return nil, err
	}
	utilizationAggregate, err := createOrOpenUtilizationAggregate(tr, db, append(taskSetPath(runnerId), "utilization_aggregate"))
	if err != nil {
		return nil, err
	}
	return &RunnerSet{set, utilizationAggregate}, nil
}

func (r *RunnerSet) Add(tx fdb.Transaction, taskKey task.TaskKey) error {
	resourceRequest, err := taskKey.ResourceRequest().Get(tx).Get()
	if err != nil {
		return err
	}
	r.utilizationAggregate.add(tx, UtilizationDimensionCPU, int64(resourceRequest.CpuMillis))
	r.utilizationAggregate.add(tx, UtilizationDimensionMemory, int64(resourceRequest.MemoryBytes))
	return r.Set.Add(tx, []byte(taskKey.Id()))
}

func (r *RunnerSet) Remove(tx fdb.Transaction, taskKey task.TaskKey) error {
	resourceRequest, err := taskKey.ResourceRequest().Get(tx).Get()
	if err != nil {
		return err
	}
	r.utilizationAggregate.add(tx, UtilizationDimensionCPU, -int64(resourceRequest.CpuMillis))
	r.utilizationAggregate.add(tx, UtilizationDimensionMemory, -int64(resourceRequest.MemoryBytes))
	return r.Set.Remove(tx, []byte(taskKey.Id()))
}

func (r *RunnerSet) GetUtilization(tx fdb.ReadTransaction, dimension UtilizationDimension) (int64, error) {
	return r.utilizationAggregate.get(tx, dimension)
}
