package f4a

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/f4a/pkg/util"
	"golang.org/x/sync/errgroup"
)

const (
	shutdownTimeout = 5 * time.Second
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
func Start(ctx context.Context, address string, executors map[string]execute.Executor) error {
	p := new(http.Protocols)
	p.SetHTTP1(true)
	// Use h2c so we can serve HTTP/2 without TLS.
	p.SetUnencryptedHTTP2(true)
	mux := http.NewServeMux()
	s := http.Server{
		Addr:      address,
		Handler:   mux,
		Protocols: p,
	}
	var shuttingDown atomic.Bool

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /readyz", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	dbr, err := util.CreateOrOpenDbRoot(fdb.MustOpenDefault(), []string{"f4a"})
	if err != nil {
		return err
	}

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
	group.Go(func() error {
		err := pool.RunWorkLoop(ctx, runnerId, dbr, taskSet, nil)

		// Signal that we're shutting down so /readyz returns 503.
		// This allows Kubernetes to stop routing new traffic to this pod.
		shuttingDown.Store(true)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if shutdownErr := s.Shutdown(shutdownCtx); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
			return errors.Join(err, shutdownErr)
		}

		return err
	})
	group.Go(func() error {
		err := s.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	return group.Wait()
}
