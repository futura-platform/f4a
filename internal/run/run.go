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
	"github.com/futura-platform/f4a/internal/reliablelock"
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
	callbackAttemptTimeout    = 10 * time.Second
	callbackDeliveryTimeout   = time.Minute
)

// Run runs the runnable singleton, identifying itself as the holder of the lock with the given runnerId.
// This uses reliablelock to ensure that only one instance of the runnable is executed at a time.
// It will re execute the runnable with the new input if the input changes before
// the current execution returns. Once execution has returned and callback
// delivery has started, later input changes do not trigger a replay.
// It will only return if:
// 1. The execution finishes successfully and the callback succeeds at least once
// 2. The execution fails and the callback succeeds at least once (delivering the error)
// 3. The parent context is canceled, which aborts any in-flight execution and callback delivery
// 4. The watch fails
func (r Runnable) Run(ctx context.Context, runnerId string, callback func(context.Context, []byte, error) error) error {
	if callback == nil {
		return errors.New("callback is required")
	}

	lock, err := r.taskKey.RunnableLock(r.db)
	if err != nil {
		return fmt.Errorf("failed to get lock: %w", err)
	}
	lease, err := lock.Acquire(ctx, r.db, reliablelock.DefaultLeaseOptions())
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	activeLease, err := lease.Activate(ctx)
	if err != nil {
		return fmt.Errorf("failed to activate lease: %w", err)
	}
	// best effort graceful release
	defer activeLease.BestEffortRelease(ctx, backoff.WithMaxElapsedTime(10*time.Second))

	// bind ctx to the lease so that operations only happen while the lease is valid
	ctx = activeLease

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

	// Once callback delivery finishes, later input changes are ignored.
	var executionResultDeliveryFinished atomic.Bool
	startExecution := func(marshalledInput []byte) {
		mu.Lock()
		if executionResultDeliveryFinished.Load() || watchCtx.Err() != nil {
			mu.Unlock()
			return
		}
		if cancelPrevious != nil {
			cancelPrevious(errInputChanged)
		}
		runCtx, runCancel := context.WithCancelCause(watchCtx)
		cancelPrevious = runCancel
		mu.Unlock() // dont hold the lock while executing. We need to be able to cancel the execution if the input changes.

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

			finishedAt, finishedAtErr := r.ensureFinishedAt(time.Now())
			if finishedAtErr != nil {
				if ctx.Err() != nil {
					return
				}
				if runErr == nil {
					runErr = finishedAtErr
				}
				watchCancel()
				return
			}

			deadline := finishedAt.Add(callbackDeliveryTimeout)
			callbackCtx, callbackCancel := context.WithDeadline(runCtx, deadline)
			defer callbackCancel()
			deliveryErr := retryCallback(
				callbackCtx,
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
				if time.Now().After(deadline) {
					flog.FromContext(ctx).LogAttrs(
						ctx, slog.LevelDebug, "callback delivery timed out",
						slog.String("task_id", string(r.Id())),
					)
				} else {
					if ctx.Err() != nil {
						return
					}
					if runErr == nil {
						runErr = deliveryErr
					}
					watchCancel()
					return
				}
			} else {
				flog.FromContext(ctx).LogAttrs(
					ctx, slog.LevelDebug, "delivered callback",
					slog.String("task_id", string(r.Id())),
					slog.Bool("error", err != nil),
				)
			}
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

func (r Runnable) ensureFinishedAt(candidate time.Time) (time.Time, error) {
	finishedAtValue, err := r.db.Transact(func(tx fdb.Transaction) (any, error) {
		existing, err := r.taskKey.FinishedAt().Get(tx).Get()
		if err != nil {
			return nil, fmt.Errorf("failed to read finishedAt: %w", err)
		}
		if existing != nil {
			return *existing, nil
		}

		finishedAt := candidate
		r.taskKey.FinishedAt().Set(tx, &finishedAt)
		return finishedAt, nil
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to persist finishedAt: %w", err)
	}
	return finishedAtValue.(time.Time), nil
}

func retryCallback(
	ctx context.Context,
	callback func(context.Context) error,
	notify func(error, time.Duration),
) error {
	delay := callbackRetryInitialDelay
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, callbackAttemptTimeout)
		err := callback(attemptCtx)
		cancel()
		if err == nil {
			return nil
		} else if notify != nil {
			notify(err, delay)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}

		delay *= 2
		if delay > callbackRetryMaxDelay {
			delay = callbackRetryMaxDelay
		}
	}
}
