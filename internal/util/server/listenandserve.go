package serverutil

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ListenAndServe wraps the ListenAndServe method of the http.Server to add a shutdown signal handler for SIGTERM and SIGINT.
func ListenAndServe(s *http.Server, shutdownTimeout time.Duration) error {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
		<-signals
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		// we ignore the error since this is a graceful shutdown, which doesn't necessarily need to be successful
		_ = s.Shutdown(ctx)
	}()
	return s.ListenAndServe()
}
