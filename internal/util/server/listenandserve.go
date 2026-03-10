package serverutil

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/f4a/internal/util"
)

// K8sAwareListenAndServe wraps the ListenAndServe method of the http.Server to add a shutdown signal handler for SIGTERM and SIGINT.
// In the event of a cancellation signal, the drain function is called with the shutdown context,
// and this function will block until the drain function returns successfully (if it is not nil).
func K8sAwareListenAndServe(s *http.Server, shutdownTimeout time.Duration, drain func(context.Context) error) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)

	stopWatcher := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		select {
		case <-signals:

			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			// We ignore shutdown errors here because the caller is responsible for
			// classifying ListenAndServe's return value.
			_ = s.Shutdown(ctx)
			if drain != nil {
				err := util.WithBestEffort(ctx, func() error {
					return drain(ctx)
				}, backoff.WithMaxElapsedTime(0))
				if err != nil {
					slog.Error("failed to drain server", "error", err)
				}
			}
		case <-stopWatcher:
			// Server stopped via a non-signal path (e.g. context/work-loop shutdown).
		}
	})

	err := s.ListenAndServe()
	close(stopWatcher)

	// wait for the shutdown watcher to finish
	wg.Wait()
	return err
}
