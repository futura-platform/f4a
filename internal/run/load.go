package run

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/fdbexec"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/execute"
)

type RunnableTask struct {
	Runnable
	// BE AWARE: this value is nullable
	callbackUrl *url.URL
}

// CallbackUrl returns the callback url for the task.
// This is nullable, and should be checked before using.
func (r RunnableTask) CallbackUrl() *url.URL {
	return r.callbackUrl
}

// LoadTasks loads the tasks from the database and returns a list of runnable tasks.
// This is mainly a helper to load a batch of tasks in a single transaction.
func LoadTasks(ctx context.Context, db dbutil.DbRoot, router execute.Router, ids []task.Id) ([]RunnableTask, error) {
	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return nil, fmt.Errorf("failed to open task directory: %w", err)
	}

	type taskLoad struct {
		id                task.Id
		taskKey           task.TaskKey
		executorIdFuture  *dbutil.Future[execute.ExecutorId]
		callbackUrlFuture *dbutil.Future[*string]
	}

	tasks := make([]RunnableTask, 0, len(ids))
	failures := make([]error, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() {
			var taskKey task.TaskKey
			var executorId execute.ExecutorId
			var callbackUrlValue *string
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				taskKey, err = tasksDirectory.Open(tx, id)
				if err != nil {
					return nil, fmt.Errorf("failed to get task key for %s: %w", id, err)
				}
				executorId = taskKey.ExecutorId().Get(tx).MustGet()
				callbackUrlValue = taskKey.CallbackUrl().Get(tx).MustGet()
				return nil, nil
			})
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					// Task assignment snapshots can briefly lag a concurrent delete.
					// Skip missing tasks so stale queue entries do not crash the worker.
					return
				}
				mu.Lock()
				failures = append(failures, fmt.Errorf("failed to load task for %s: %w", id, err))
				mu.Unlock()
				return
			}
			executor := router.Route(executorId)
			var callbackUrl *url.URL
			if callbackUrlValue != nil {
				callbackUrl, err = url.Parse(*callbackUrlValue)
				if err != nil {
					mu.Lock()
					failures = append(failures, fmt.Errorf("failed to parse callback url %s: %w", id, err))
					mu.Unlock()
					return
				}
			}

			mu.Lock()
			tasks = append(
				tasks,
				RunnableTask{
					Runnable: NewRunnable(
						executor,
						executorId,
						db.Database,
						taskKey,
						fdbexec.NewContainer(id, db),
					),
					callbackUrl: callbackUrl,
				},
			)
			mu.Unlock()
		})
	}
	wg.Wait()
	if len(failures) > 0 {
		return nil, fmt.Errorf("failed to load %d/%d tasks: %v", len(failures), len(ids), failures)
	}

	return tasks, nil
}
