package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// DrainTaskRunner marks the runner as inactive and moves all tasks from its task set
// back to the pending set so they can be re-assigned by the dispatch service.
// It is idempotent: tasks already moved off the runner or deleted are skipped.
func DrainTaskRunner(
	ctx context.Context,
	dbr dbutil.DbRoot,
	runnerId string,
	activeRunners ActiveRunners,
	taskSet *reliableset.Set,
	pendingSet *reliableset.Set,
	taskDir task.TasksDirectory,
) error {
	// immediately mark runner as inactive when draining the pod.
	var hangingTasks mapset.Set[string]
	_, err := dbr.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		activeRunners.SetActive(tx, runnerId, false)
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to mark runner as inactive: %w", err)
	}

	// fetch the task set items to be drained, in a separate transaction,
	// so failures here do not block the runner from being marked as inactive.
	_, err = dbr.ReadTransactContext(ctx, func(tx fdb.ReadTransaction) (any, error) {
		// since the task set is unbounded, this can overload the tx size limit.
		// this is an acceptable compromise for now.
		// TODO: implement an iterator in reliableset so cases like this can be properly handled.
		tasks, _, err := taskSet.Items(tx)
		if err != nil {
			return nil, fmt.Errorf("failed to get task set items: %w", err)
		}
		hangingTasks = tasks
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to mark runner as inactive: %w", err)
	}

	// do a best effort to drain the task set, using batching to avoid overloading the tx size limit.
	const drainBatchSize = 256
	for hangingTasks.Cardinality() > 0 {
		currentBatch := mapset.NewSet[string]()
		for range drainBatchSize {
			taskID, ok := hangingTasks.Pop()
			if !ok {
				break
			}
			currentBatch.Add(taskID)
		}
		_, err = dbr.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
			for taskID := range currentBatch.Iter() {
				tkey, err := taskDir.Open(tx, task.Id(taskID))
				if err != nil {
					if errors.Is(err, directory.ErrDirNotExists) {
						// Task already completed/deleted while draining; skip idempotently.
						continue
					}
					return nil, fmt.Errorf("failed to open task: %w", err)
				}

				assignmentState, err := task.ReadAssignmentState(tx, tkey)
				if err != nil {
					return nil, fmt.Errorf("failed to read task assignment state: %w", err)
				}
				if err := assignmentState.ValidateRunnerLifecycleInvariant(); err != nil {
					return nil, fmt.Errorf("task assignment invariant violation: %w", err)
				}
				isRunningOnThisRunner, err := assignmentState.IsRunningOn(runnerId)
				if err != nil {
					return nil, err
				}
				if !isRunningOnThisRunner {
					// Task has been moved off this runner already. skip idempotently.
					continue
				}

				err = taskSet.Remove(tx, []byte(taskID))
				if err != nil {
					return nil, fmt.Errorf("failed to remove task from task set: %w", err)
				}
				err = pendingSet.Add(tx, []byte(taskID))
				if err != nil {
					return nil, fmt.Errorf("failed to add task to pending set: %w", err)
				}
				tkey.LifecycleStatus().Set(tx, task.LifecycleStatusPending)
				tkey.RunnerId().Set(tx, nil)
			}
			return nil, nil
		})
		if err != nil {
			return fmt.Errorf("failed to clear task set: %w", err)
		}
		hangingTasks.RemoveAll(currentBatch.ToSlice()...)
	}

	return taskSet.Clear()
}
