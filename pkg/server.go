package f4a

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/util"
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
	port, err := util.RequiredPort("WORKER_PORT")
	if err != nil {
		return err
	}
	return startOnAddress(ctx, fmt.Sprintf(":%d", port), executors)
}

func startOnAddress(ctx context.Context, address string, executors map[string]execute.Executor) error {
	dbr, err := util.CreateOrOpenDefaultDbRoot()
	if err != nil {
		return err
	}
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

	taskSet, err := pool.CreateOrOpenTaskSetForRunner(dbr, runnerId)
	if err != nil {
		return err
	}

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

	group.Go(func() error {
		err := pool.RunWorkLoop(ctx, runnerId, dbr, taskSet, router)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), constants.SHUTDOWN_TIMEOUT)
		defer cancel()
		if shutdownErr := s.Shutdown(shutdownCtx); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
			return errors.Join(err, shutdownErr)
		}

		return err
	})
	group.Go(func() error {
		err := serverutil.ListenAndServe(s, constants.SHUTDOWN_TIMEOUT)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	return group.Wait()
}
