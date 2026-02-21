package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablewatch"
	"github.com/futura-platform/futura/flog"
)

var (
	errInputChanged = errors.New("input changed")
	// this error will cause the run to return the error instead of just calling the callback with the error.
	// This is for testing purposes ONLY.
	ErrRunFatal = errors.New("run encountered fatal error")
)

const (
	callbackRetryInitialDelay = 100 * time.Millisecond
	callbackRetryMaxDelay     = 30 * time.Second
	callbackTimeout           = 10 * time.Second
)

var callbackRetrySleep = time.Sleep

// Run runs the runnable singleton, identifying itself as the holder of the lock with the given runnerId.
// This uses reliablelock to ensure that only one instance of the runnable is executed at a time.
// It will re execute the runnable with the new input if the input changes.
// It will only return if either:
// 1. The execution finishes successfully and the callback succeeds at least once
// 2. The execution fails and the callback succeeds at least once with the error
func (r Runnable) Run(ctx context.Context, runnerId string, callback func(context.Context, []byte, error) error) error {
	if callback == nil {
		return errors.New("callback is required")
	}

	lock := r.taskKey.RunnableLock(r.db, runnerId)
	err := lock.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to lock task: %w", err)
	}
	defer lock.Release()

	executable := r.executor.ExecuteFrom(r.execution)
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	inputKey := r.taskKey.Input()
	valuesCh, errCh := reliablewatch.WatchCh(
		watchCtx,
		r.db,
		inputKey.Key(),
		nil,
		nil,
		func(t fdb.ReadTransaction, _ fdb.KeyConvertible, _ []byte) ([]byte, error) {
			return inputKey.Get(t).Get()
		},
	)

	var mu sync.Mutex
	var cancelPrevious context.CancelCauseFunc
	var success bool

	var runErr error
	var execWg sync.WaitGroup

	startExecution := func(marshalledInput []byte) {
		mu.Lock()
		if cancelPrevious != nil {
			cancelPrevious(errInputChanged)
		}
		runCtx, runCancel := context.WithCancelCause(watchCtx)
		cancelPrevious = runCancel
		mu.Unlock() // dont hold the lock while executing the execution. We need to be able to cancel the execution if the input changes.

		execWg.Add(1)
		go func(input []byte, runCtx context.Context) {
			defer execWg.Done()
			result, err := executable.Execute(runCtx, input)
			if errors.Is(err, ErrRunFatal) {
				if !testing.Testing() {
					panic(fmt.Errorf("This error should never be used outside of tests: %w", err))
				}
				mu.Lock()
				if runErr == nil {
					runErr = err
				}
				mu.Unlock()
				watchCancel()
				return
			} else if errors.Is(err, errInputChanged) {
				return
			}

			mu.Lock()
			defer mu.Unlock()
			watchCancel()
			err = retryCallback(ctx, callback, result, err, callbackTimeout)
			flog.FromContext(ctx).LogAttrs(
				ctx, slog.LevelDebug, "delivered callback",
				slog.String("task_id", string(r.Id())),
				slog.Bool("error", err == nil),
			)
			success = true
		}(marshalledInput, runCtx)
	}

	for {
		select {
		case marshalledInput, ok := <-valuesCh:
			if !ok {
				valuesCh = nil
				continue
			}
			startExecution(marshalledInput)
		case err, ok := <-errCh:
			if !ok {
				err = nil
			}
			execWg.Wait()
			mu.Lock()
			defer mu.Unlock()
			if runErr != nil {
				return runErr
			}
			if success {
				return nil
			}
			return err
		}
	}
}

func retryCallback(
	ctx context.Context,
	callback func(context.Context, []byte, error) error,
	output []byte,
	runErr error,
	attemptTimeout time.Duration,
) error {
	delay := callbackRetryInitialDelay
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err := callback(attemptCtx, output, runErr)
		cancel()
		if err == nil {
			return nil
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
		callbackRetrySleep(delay)
		delay *= 2
		if delay > callbackRetryMaxDelay {
			delay = callbackRetryMaxDelay
		}
	}
}
