package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

// DrainTaskRunner marks the runner as inactive and moves all tasks from its task set
// back to the pending set so they can be re-assigned by the dispatch service.
// DrainTaskRunner marks a runner as inactive and drains its assigned tasks to the pending set. Tasks that are already moved off the runner or have been deleted are skipped.
func DrainTaskRunner(
	ctx context.Context,
	dbr dbutil.DbRoot,
	placer *servicestate.TaskPlacer,
	runnerId string,
	activeRunners ActiveRunners,
	taskSet *servicestate.RunnerSet,
	taskDir task.TasksDirectory,
) error {
	// immediately mark runner as inactive when draining the pod.
	_, err := dbr.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		activeRunners.SetActive(tx, runnerId, false)
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to mark runner as inactive: %w", err)
	}

	hangingTasks, _, err := taskSet.Items(ctx, dbr.Database)
	if err != nil {
		return fmt.Errorf("failed to get task set items: %w", err)
	}

	// do a best effort to drain the task set, using batching to avoid overloading the tx size limit.
	const drainBatchSize = 128
	for hangingTasks.Cardinality() > 0 {
		currentBatch := mapset.NewSet[task.Id]()
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
				if err := assignmentState.ValidateRunnerIdInvariant(); err != nil {
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

				err = placer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, tkey)
				if err != nil {
					return nil, fmt.Errorf("failed to place task in pending set: %w", err)
				}
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
