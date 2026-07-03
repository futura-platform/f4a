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

var deleteTaskAfterCallback = func(ctx context.Context, manager *taskManager, runnable run.RunnableTask) error {
	return manager.deleteTaskAfterCallback(ctx, runnable)
}

// run shadows the runMap.run method. This is to abstract away the callback function.
func (m *taskManager) run(ctx context.Context, r run.RunnableTask) error {
	return m.runMap.run(ctx, r.Runnable, r.CallbackUrl())
}

func (m *taskManager) deleteTaskAfterCallback(ctx context.Context, runnable run.RunnableTask) error {
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "deleting task after callback",
		slog.String("task_id", string(runnable.Id())))
	_, err := m.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		taskKey, err := m.taskDirectory.Open(tx, runnable.Id())
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

		_, err = m.revisionStore.ApplyNext(tx, runnable.Id(), task.RevisionOperationDelete, func() error {
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
