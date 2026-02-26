package f4a

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/internal/util"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	serverutil "github.com/futura-platform/f4a/internal/util/server"
	"github.com/futura-platform/f4a/pkg/constants"
	"github.com/futura-platform/f4a/pkg/execute"
	"golang.org/x/sync/errgroup"
)

// Start starts an F4A worker + an associated health probe server.
// It will route tasks via the given executors.
// This function will block until the work loop encounters an error.
// If you need to gracefully shutdown the worker (cancel all tasks and wait for them to finish),
// you should cancel the context that was provided to this function,
// and expect a context.Canceled error.
// The caller is expected to handle graceful shutdown of this worker.
//
// This is designed to run in Kubernetes as a StatefulSet pod.
// See: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
func Start(ctx context.Context, executors map[string]execute.Executor) error {
	port, err := util.RequiredPort(constants.WorkerPort)
	if err != nil {
		return err
	}
	return startOnAddress(ctx, fmt.Sprintf(":%d", port), executors)
}

func startOnAddress(ctx context.Context, address string, executors map[string]execute.Executor) (err error) {
	dbr, err := dbutil.CreateOrOpenDefaultDbRoot()
	if err != nil {
		return err
	}
	ctx = dbutil.WithDB(ctx, dbr)

	s, _ := serverutil.NewBaseK8sService(dbr, func() (status int) {
		return http.StatusOK
	}, func() (status int) {
		return http.StatusOK
	})
	s.Addr = address

	// StatefulSet pods have unique, stable hostnames.
	runnerId, err := os.Hostname()
	if err != nil {
		return err
	}
	taskSet, cancelTaskSet, err := pool.CreateOrOpenTaskSetForRunner(dbr, runnerId)
	if err != nil {
		return err
	}
	defer cancelTaskSet()

	group, ctx := errgroup.WithContext(ctx)
	router := execute.NewRouter()
	if len(executors) > 0 {
		routes := make([]execute.Route, 0, len(executors))
		for id, executor := range executors {
			routes = append(routes, execute.Route{
				Id:       execute.ExecutorId(id),
				Executor: executor,
			})
		}
		router = execute.NewRouter(routes...)
	}
	var workLoopInitiatedShutdown atomic.Bool

	activeRunners, err := pool.CreateOrOpenActiveRunners(dbr)
	if err != nil {
		return err
	}
	_, err = dbr.Transact(func(tx fdb.Transaction) (any, error) {
		activeRunners.SetActive(tx, runnerId, true)
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to mark runner as active: %w", err)
	}

	workLoopCtx, cancelWorkLoop := context.WithCancel(ctx)
	defer cancelWorkLoop()
	group.Go(func() error {
		err := pool.RunWorkLoop(workLoopCtx, runnerId, dbr, taskSet, router)
		workLoopInitiatedShutdown.Store(true)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), constants.SHUTDOWN_TIMEOUT)
		defer cancel()
		if shutdownErr := s.Shutdown(shutdownCtx); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
			return errors.Join(err, shutdownErr)
		}

		return err
	})
	group.Go(func() error {
		pendingSet, cancelPendingSet, err := servicestate.CreateOrOpenReadySet(dbr)
		if err != nil {
			return fmt.Errorf("failed to open pending set: %w", err)
		}
		cancelPendingSet()
		taskDir, err := task.CreateOrOpenTasksDirectory(dbr)
		if err != nil {
			return fmt.Errorf("failed to open task directory: %w", err)
		}
		err = serverutil.K8sAwareListenAndServe(s, constants.SHUTDOWN_TIMEOUT, func() error {
			cancelWorkLoop()

			// immediately mark runner as inactive when draining the pod.
			var hangingTasks mapset.Set[string]
			_, err := dbr.Transact(func(tx fdb.Transaction) (any, error) {
				activeRunners.SetActive(tx, runnerId, false)
				return nil, nil
			})
			if err != nil {
				return fmt.Errorf("failed to mark runner as inactive: %w", err)
			}

			// fetch the task set items to be drained, in a separate transaction,
			// so failures here do not block the runner from being marked as inactive.
			_, err = dbr.Transact(func(tx fdb.Transaction) (any, error) {
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

			// tasks that remain on this instance will be re-assigned to a different instance by the dispatch service.
			// TODO: implement draining in the dispatch service. consider extracting the drain logic into a shared helper function.

			// but for now, we will do a best effort to drain the task set.
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
				_, err = dbr.Transact(func(tx fdb.Transaction) (any, error) {
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
			return err
		})
		return resolveServerLoopExit(err, workLoopInitiatedShutdown.Load())
	})

	return group.Wait()
}

func resolveServerLoopExit(err error, workLoopInitiatedShutdown bool) error {
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if workLoopInitiatedShutdown {
		// Work loop requested shutdown (e.g. internal error). Preserve the work-loop error.
		return nil
	}
	// HTTP server closed externally (e.g. SIGTERM). Cancel the work loop context
	// via errgroup without deleting queue assignment state.
	return context.Canceled
}
