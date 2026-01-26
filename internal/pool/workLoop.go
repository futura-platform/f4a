package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/f4a/pkg/util"
	"github.com/futura-platform/futura/flog"
)

var (
	ErrRunFailed   = errors.New("run failed")
	ErrWatchFailed = errors.New("failed to watch task assignments")
)

type taskManager struct {
	*runMap
	c *http.Client
}

// RunWorkLoop handles the task execution management for a pool.
// It watches the task assignments assigned to the pool
// (which is implied to be scoped to the given dbRoot),
// and starts and stops the execution of tasks as needed.
// It runs a runnable in a separate goroutine for each new task,
// and stops them when they are no longer assigned to the pool.
// TODO: add options to this function (first need the ability attach a logger)
func RunWorkLoop(
	ctx context.Context,
	runnerId string,
	db util.DbRoot,
	taskSet *reliableset.Set,
	router execute.Router,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var runErrOnce sync.Once
	runErrCh := make(chan error, 1)
	taskManager := &taskManager{
		runMap: newRunMap(runnerId, func(id task.Id, err error) {
			runErrOnce.Do(func() {
				runErrCh <- fmt.Errorf("%w: %s: %w", ErrRunFailed, id, err)
			})
		}),
		c: http.DefaultClient,
	}
	defer taskManager.cancelAll()

	initialValues, eventsCh, streamErrCh, err := taskSet.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create task set stream: %w", err)
	}

	if len(initialValues) > 0 {
		err = processAddedBatch(ctx, taskManager, db, router, initialValues)
		if err != nil {
			return fmt.Errorf("failed to process initial values: %w", err)
		}
	}

	for {
		select {
		case err := <-runErrCh:
			return err
		case err := <-streamErrCh:
			if err != nil {
				return fmt.Errorf("task set failed: %w", err)
			}
			return nil
		case b := <-eventsCh:
			switch b.Type {
			case reliableset.StreamEventTypeAdded:
				err = processAddedBatch(ctx, taskManager, db, router, b.Items)
				if err != nil {
					return fmt.Errorf("failed to process added batch: %w", err)
				}
			case reliableset.StreamEventTypeRemoved:
				err = processRemovedBatch(taskManager.runMap, b.Items)
				if err != nil {
					return fmt.Errorf("failed to process removed batch: %w", err)
				}
			default:
				return fmt.Errorf("unknown stream event type: %d", b.Type)
			}
		}
	}
}

func processAddedBatch(
	ctx context.Context,
	taskManager *taskManager,
	db util.DbRoot,
	router execute.Router,
	items [][]byte,
) error {
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "processing added batch",
		slog.String("item_count", fmt.Sprintf("%d", len(items))))

	taskIds := make([]task.Id, len(items))
	for i, item := range items {
		id, err := task.IdFromBytes(item)
		if err != nil {
			return fmt.Errorf("failed to parse task id: %w", err)
		}
		taskIds[i] = id
	}
	runnables, err := run.LoadTasks(ctx, db, router, taskIds)
	if err != nil {
		return fmt.Errorf("failed to load runnables: %w", err)
	}
	for _, runnable := range runnables {
		err := taskManager.run(runnable)
		if err != nil {
			return fmt.Errorf("failed to run task %s: %w", runnable.Id(), err)
		}
	}
	return nil
}

func processRemovedBatch(taskManager *runMap, items [][]byte) error {
	for _, item := range items {
		id, err := task.IdFromBytes(item)
		if err != nil {
			return fmt.Errorf("failed to parse task id: %w", err)
		}
		err = taskManager.cancel(id)
		if err != nil {
			return fmt.Errorf("failed to cancel task %s: %w", id, err)
		}
	}
	return nil
}
