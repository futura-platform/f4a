package run

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/f4a/internal/reliablelock"
	"github.com/futura-platform/f4a/internal/reliablewatch"
	"github.com/futura-platform/futura/fopt"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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

var (
	tracer = otel.Tracer("f4a.runner.run")
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
func (r Runnable) Run(ctx context.Context, runnerId string, callbackUrl *url.URL) error {
	if callbackUrl == nil {
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

	executable := r.executor.ExecuteFrom(r.userContainer)

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

	var mu, execSingleflightMu sync.Mutex
	var cancelPrevious context.CancelCauseFunc

	var runErr error
	var execWg sync.WaitGroup

	span := trace.SpanFromContext(ctx)
	startExecution := func(marshalledInput []byte) {
		mu.Lock()
		if watchCtx.Err() != nil {
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

			execSingleflightMu.Lock()
			callbackDeliveryFailure, err := executable.Execute(runCtx, input, callbackUrl, fopt.WithStepWrapper(func(
				ctx context.Context,
				fnLabel string,
				args any,
				callstack []runtime.Frame,
				call func() (output any, err error),
			) (errOverride error) {
				ctx, span := tracer.Start(ctx, fnLabel)
				defer span.End()
				span.SetAttributes(attribute.String("label", fnLabel))
				_, err := call()
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				return nil
			}))
			execSingleflightMu.Unlock()

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
			}

			if callbackDeliveryFailure.IsSome() {
				// todo: handle callback delivery failure with a dead letter queue
			}

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
			span.AddEvent("input changed")
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
			return err
		}
	}
}
