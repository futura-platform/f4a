package f4a

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
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

type StartOptions struct {
	// Drain function is called when the server is gracefully shutting down from being SIGTERM'd.
	// If it gets an error, it will be retried until this instance is forcefully killed.
	Drain func(context.Context) error
}

func WithDrain(drain func(context.Context) error) StartOption {
	return func(options *StartOptions) {
		options.Drain = drain
	}
}

type StartOption func(*StartOptions)

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
func Start(ctx context.Context, executors map[string]execute.Executor, options ...StartOption) error {
	port, err := util.RequiredPort(constants.WorkerPort)
	if err != nil {
		return err
	}
	return startOnAddress(ctx, fmt.Sprintf(":%d", port), executors, options...)
}

func startOnAddress(ctx context.Context, address string, executors map[string]execute.Executor, opts ...StartOption) (err error) {
	options := new(StartOptions)
	for _, o := range opts {
		o(options)
	}

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
	taskSet, err := pool.CreateOrOpenTaskSetForRunner(dbr, dbr, runnerId)
	if err != nil {
		return err
	}
	cancelTaskSetCompaction := taskSet.RunCompactor()
	defer cancelTaskSetCompaction()

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
		pendingSet, err := servicestate.CreateOrOpenReadySet(dbr, dbr)
		if err != nil {
			return fmt.Errorf("failed to open pending set: %w", err)
		}
		taskDir, err := task.CreateOrOpenTasksDirectory(dbr)
		if err != nil {
			return fmt.Errorf("failed to open task directory: %w", err)
		}
		taskRunnerDrained := false
		err = serverutil.K8sAwareListenAndServe(s, constants.SHUTDOWN_TIMEOUT, func(shutdownCtx context.Context) error {
			if !taskRunnerDrained {
				cancelWorkLoop()
				err := pool.DrainTaskRunner(shutdownCtx, dbr, runnerId, activeRunners, taskSet, pendingSet, taskDir)
				if err != nil {
					return fmt.Errorf("failed to drain task runner: %w", err)
				}
				taskRunnerDrained = true
			}
			if options.Drain != nil {
				return options.Drain(shutdownCtx)
			}
			return nil
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
