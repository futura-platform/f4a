package run

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/fdbexec"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/f4a/pkg/util"
	dbutil "github.com/futura-platform/f4a/pkg/util/db"
)

type RunnableTask struct {
	Runnable
	callbackUrl *url.URL
}

func (r RunnableTask) CallbackUrl() *url.URL {
	return r.callbackUrl
}

// LoadTasks loads the tasks from the database and returns a list of runnable tasks.
// This is mainly a helper to load a batch of tasks in a single transaction.
func LoadTasks(ctx context.Context, db util.DbRoot, router execute.Router, ids []task.Id) ([]RunnableTask, error) {
	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return nil, fmt.Errorf("failed to open task directory: %w", err)
	}

	executorIdFutures := make([]*dbutil.Future[execute.ExecutorId], len(ids))
	callbackUrlFutures := make([]*dbutil.Future[string], len(ids))
	taskKeys := make([]task.TaskKey, len(ids))
	for i, id := range ids {
		tkey, err := tasksDirectory.TaskKey(db, id)
		if err != nil {
			return nil, fmt.Errorf("failed to get task key for %s: %w", id, err)
		}
		taskKeys[i] = tkey
	}
	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		for i := range ids {
			executorIdFutures[i] = taskKeys[i].ExecutorId().Get(tx)
			callbackUrlFutures[i] = taskKeys[i].CallbackUrl().Get(tx)
		}
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to transact: %w", err)
	}

	tasks := make([]RunnableTask, 0, len(ids))
	failures := make([]error, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range len(ids) {
		id := ids[i]
		executorIdFuture := executorIdFutures[i]
		callbackUrlFuture := callbackUrlFutures[i]
		wg.Go(func() {
			executorId, err := executorIdFuture.Get()
			if err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("failed to get executor id %s: %w", id, err))
				mu.Unlock()
				return
			}

			executor, err := router.Route(executorId)
			if err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("failed to route executor %s: %w", id, err))
				mu.Unlock()
				return
			}

			callbackUrlValue, err := callbackUrlFuture.Get()
			if err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("failed to get callback url %s: %w", id, err))
				mu.Unlock()
				return
			}
			callbackUrl, err := url.Parse(callbackUrlValue)
			if err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("failed to parse callback url %s: %w", id, err))
				mu.Unlock()
				return
			}

			tkey, err := tasksDirectory.TaskKey(db, id)
			if err != nil {
				mu.Lock()
				failures = append(failures, fmt.Errorf("failed to get task key %s: %w", id, err))
				mu.Unlock()
				return
			}

			mu.Lock()
			tasks = append(
				tasks,
				RunnableTask{
					Runnable: NewRunnable(
						executor,
						db.Database,
						tkey,
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
