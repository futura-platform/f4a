package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/futura/flog"
)

// run shadows the runMap.run method.
func (m *taskManager) run(ctx context.Context, r run.RunnableTask) error {
	return m.runMap.run(ctx, r.Runnable, r.CallbackUrl())
}

// deleteTask is indirected through a package var so tests can inject
// failures/races into the retirement path.
var deleteTask = func(ctx context.Context, m *taskManager, id task.Id) error {
	return m.deleteTask(ctx, id)
}

func (m *taskManager) deleteTask(ctx context.Context, id task.Id) error {
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "deleting settled task",
		slog.String("task_id", string(id)))
	_, err := m.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		taskKey, err := m.taskDirectory.Open(tx, id)
		if err != nil {
			if errors.Is(err, directory.ErrDirNotExists) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to open task: %w", err)
		}
		assignmentState, err := task.ReadAssignmentState(tx, taskKey)
		if err != nil {
			return nil, err
		}
		if err := assignmentState.ValidateRunnerIdInvariant(); err != nil {
			return nil, fmt.Errorf("task assignment invariant violation: %w", err)
		}
		isRunningOnThisRunner, err := assignmentState.IsRunningOn(m.runnerId)
		if err != nil {
			return nil, err
		} else if !isRunningOnThisRunner {
			return nil, nil
		}

		_, err = m.revisionStore.ApplyNext(tx, id, task.RevisionOperationDelete, func() error {
			if err := m.placer.PlaceTaskIn(tx, servicestate.PlacementLocationNowhere, taskKey); err != nil {
				return fmt.Errorf("failed to remove task from task queue: %w", err)
			}
			if err := taskKey.Clear(tx); err != nil {
				return fmt.Errorf("failed to clear task: %w", err)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to apply revisioned delete: %w", err)
		}
		return nil, nil
	})
	return err
}
