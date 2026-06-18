package pool

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// RunnerSet is a helper wrapper around a set that ensures the utilization aggregate is updated whenever the set is mutated.
type RunnerSet struct {
	*reliableset.Set
	utilizationAggregate *utilizationAggregate
	taskOwnership        directory.DirectorySubspace
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
	taskOwnership, err := db.Root.Open(tr, append(taskSetPath(runnerId), "task_ownership"), nil)
	if err != nil {
		return nil, err
	}
	return &RunnerSet{set, utilizationAggregate, taskOwnership}, nil
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
	taskOwnership, err := db.Root.CreateOrOpen(tr, append(taskSetPath(runnerId), "task_ownership"), nil)
	if err != nil {
		return nil, err
	}
	return &RunnerSet{set, utilizationAggregate, taskOwnership}, nil
}

func (r *RunnerSet) taskOwnershipKey(id task.Id) fdb.Key {
	return r.taskOwnership.Pack(tuple.Tuple{string(id)})
}

func (r *RunnerSet) ownsTask(tx fdb.ReadTransaction, id task.Id) (bool, error) {
	bytes, err := tx.Get(r.taskOwnershipKey(id)).Get()
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

func (r *RunnerSet) setTaskOwnership(tx fdb.Transaction, id task.Id, owned bool) {
	if owned {
		tx.Set(r.taskOwnershipKey(id), []byte{1})
		return
	}
	tx.Clear(r.taskOwnershipKey(id))
}

func (r *RunnerSet) Add(tx fdb.Transaction, taskKey task.TaskKey) error {
	owned, err := r.ownsTask(tx, taskKey.Id())
	if err != nil {
		return err
	}
	if owned {
		return nil
	}

	resourceRequest, err := taskKey.ResourceRequest().Get(tx).Get()
	if err != nil {
		return err
	}
	r.utilizationAggregate.add(tx, UtilizationDimensionCPU, int64(resourceRequest.CpuMillis))
	r.utilizationAggregate.add(tx, UtilizationDimensionMemory, int64(resourceRequest.MemoryBytes))
	r.setTaskOwnership(tx, taskKey.Id(), true)
	return r.Set.Add(tx, []byte(taskKey.Id()))
}

func (r *RunnerSet) Remove(tx fdb.Transaction, taskKey task.TaskKey) error {
	owned, err := r.ownsTask(tx, taskKey.Id())
	if err != nil {
		return err
	}
	if !owned {
		return nil
	}

	resourceRequest, err := taskKey.ResourceRequest().Get(tx).Get()
	if err != nil {
		return err
	}
	r.utilizationAggregate.add(tx, UtilizationDimensionCPU, -int64(resourceRequest.CpuMillis))
	r.utilizationAggregate.add(tx, UtilizationDimensionMemory, -int64(resourceRequest.MemoryBytes))
	r.setTaskOwnership(tx, taskKey.Id(), false)
	return r.Set.Remove(tx, []byte(taskKey.Id()))
}

func (r *RunnerSet) GetUtilization(tx fdb.ReadTransaction, dimension UtilizationDimension) (int64, error) {
	return r.utilizationAggregate.get(tx, dimension)
}
