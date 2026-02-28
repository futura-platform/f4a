package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/f4a/internal/reliablewatch"
	"github.com/futura-platform/futura/flog"
)

var (
	errInputChanged = errors.New("input changed")
	// this error will cause the run to return the error instead of just calling the callback with the error.
	// This is for testing purposes ONLY.
	ErrRunFatal        = errors.New("run encountered fatal error")
	ErrCallbackTimeout = errors.New("callback delivery timed out")
)

// Run runs the runnable singleton, identifying itself as the holder of the lock with the given runnerId.
// This uses reliablelock to ensure that only one instance of the runnable is executed at a time.
// It will re execute the runnable with the new input if the input changes.
// It will only return if:
// 1. The execution finishes successfully and the callback succeeds at least once
// 2. The execution fails and the callback succeeds at least once (delivering the error)
// 3. The callback fails to deliver the result within the callback timeout budget
// 4. The parent context is canceled, which aborts any in-flight execution and callback delivery
// 5. The watch fails
func (r Runnable) Run(
	ctx context.Context,
	runnerId string,
	callback func(context.Context, []byte, error) error,
	callbackTimeout time.Duration,
) error {
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

	var runErr error
	var execWg sync.WaitGroup

	var executionResultDeliveryFinished atomic.Bool
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
			} else if errors.Is(context.Cause(runCtx), errInputChanged) {
				return
			}

			mu.Lock()
			defer mu.Unlock()
			deliveryErr := retryCallbackUntilTimeout(
				runCtx,
				callbackTimeout,
				func(callbackCtx context.Context) error {
					return callback(callbackCtx, result, err)
				},
				func(err error, duration time.Duration) {
					flog.FromContext(watchCtx).LogAttrs(
						watchCtx, slog.LevelDebug, "callback failed, retrying",
						slog.String("task_id", string(r.Id())),
						slog.String("error", err.Error()),
						slog.Duration("duration", duration),
					)
				},
			)
			if deliveryErr != nil {
				// Cancellation here means either a newer input superseded this run or the parent
				// context requested shutdown. In either case, do not mark delivery as finished.
				if errors.Is(deliveryErr, context.Canceled) {
					return
				}
				if runErr == nil {
					runErr = deliveryErr
				}
				watchCancel()
				return
			}
			flog.FromContext(ctx).LogAttrs(
				ctx, slog.LevelDebug, "delivered callback",
				slog.String("task_id", string(r.Id())),
				slog.Bool("error", err != nil),
			)
			executionResultDeliveryFinished.Store(true)
			watchCancel()
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
			} else if executionResultDeliveryFinished.Load() {
				// if the execution result delivery finished, we should ignore the error
				return nil
			}
			return err
		}
	}
}

func retryCallbackUntilTimeout(
	ctx context.Context,
	callbackTimeout time.Duration,
	callback func(context.Context) error,
	notify func(error, time.Duration),
) error {
	if callbackTimeout <= 0 {
		return fmt.Errorf("%w: invalid timeout %s", ErrCallbackTimeout, callbackTimeout)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, callbackTimeout)
	defer cancel()

	b := backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(0))
	var lastErr error
	for {
		err := callback(timeoutCtx)
		if err == nil {
			return nil
		}
		lastErr = err

		if err := timeoutCtx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("%w: %w", ErrCallbackTimeout, lastErr)
			}
			return err
		}

		next := b.NextBackOff()
		if notify != nil {
			notify(lastErr, next)
		}

		timer := time.NewTimer(next)
		select {
		case <-timeoutCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("%w: %w", ErrCallbackTimeout, lastErr)
			}
			return timeoutCtx.Err()
		case <-timer.C:
		}
	}
}
