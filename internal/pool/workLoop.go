package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/execute"
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
	db dbutil.DbRoot,
	taskSet *reliableset.Set,
	router execute.Router,
) error {
	ctx, cancel := context.WithCancel(ctx)

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
	defer func() {
		cancel()
		taskManager.wait()
	}()

	initialValues, eventsCh, streamErrCh, err := taskSet.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create task set stream: %w", err)
	}

	if initialValues.Cardinality() > 0 {
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
			// record all the changes that happened in this batch
			// (a key can either be in the added set or the removed set, but NOT both)
			addedSet := mapset.NewSetWithSize[string](len(b))
			removedSet := mapset.NewSetWithSize[string](len(b))
			for _, item := range b {
				v := string(item.Value)
				switch item.Op {
				case reliableset.LogOperationAdd:
					addedSet.Add(v)
					removedSet.Remove(v)
				case reliableset.LogOperationRemove:
					removedSet.Add(v)
					addedSet.Remove(v)
				default:
					return fmt.Errorf("unknown log operation: %d", item.Op)
				}
			}
			// then apply the changes as a batch
			if addedSet.Cardinality() > 0 {
				err = processAddedBatch(ctx, taskManager, db, router, addedSet)
				if err != nil {
					return fmt.Errorf("failed to process added batch: %w", err)
				}
			}

			if removedSet.Cardinality() > 0 {
				err = processRemovedBatch(taskManager.runMap, removedSet)
				if err != nil {
					return fmt.Errorf("failed to process removed batch: %w", err)
				}
			}
		}
	}
}

func processAddedBatch(
	ctx context.Context,
	taskManager *taskManager,
	db dbutil.DbRoot,
	router execute.Router,
	items mapset.Set[string],
) error {
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "processing added batch",
		slog.String("item_count", fmt.Sprintf("%d", items.Cardinality())))

	taskIds := make([]task.Id, items.Cardinality())
	for i, item := range items.ToSlice() {
		id, err := task.IdFromBytes([]byte(item))
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
		err := taskManager.run(ctx, runnable)
		if err != nil {
			return fmt.Errorf("failed to run task %s: %w", runnable.Id(), err)
		}
	}
	return nil
}

func processRemovedBatch(taskManager *runMap, items mapset.Set[string]) error {
	for _, item := range items.ToSlice() {
		id, err := task.IdFromBytes([]byte(item))
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
