package run

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/pkg/util"
	testutil "github.com/futura-platform/f4a/pkg/util/test"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
)

func setInput(t *testing.T, db util.DbRoot, td task.TaskKey, value []byte) {
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		td.Input().Set(tx, value)
		return nil, nil
	})
	assert.NoError(t, err)
}

func TestRun(t *testing.T) {
	t.Run("returns output on successful execution", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			expectedOutput := []byte("success output")
			inputData := []byte("test input")
			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, inputData)

			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					return expectedOutput, nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			outputCh := make(chan []byte, 1)
			callback := func(_ context.Context, output []byte, err error) error {
				assert.NoError(t, err)
				outputCh <- output
				return nil
			}

			err = runnable.Run(t.Context(), t.Name(), callback)
			assert.NoError(t, err)
			select {
			case output := <-outputCh:
				assert.Equal(t, expectedOutput, output)
			default:
				t.Fatal("callback not called")
			}
		})
	})

	t.Run("retries callback until it succeeds", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			expectedOutput := []byte("success output")
			inputData := []byte("test input")
			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, inputData)

			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					return expectedOutput, nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			originalSleep := callbackRetrySleep
			var sleepDurations []time.Duration
			callbackRetrySleep = func(d time.Duration) {
				sleepDurations = append(sleepDurations, d)
			}
			defer func() {
				callbackRetrySleep = originalSleep
			}()

			attempts := atomic.Int32{}
			outputCh := make(chan []byte, 1)
			callback := func(_ context.Context, output []byte, err error) error {
				assert.NoError(t, err)
				attempt := attempts.Add(1)
				if attempt < 3 {
					return errors.New("callback failed")
				}
				outputCh <- output
				return nil
			}

			err = runnable.Run(t.Context(), t.Name(), callback)
			assert.NoError(t, err)
			assert.Equal(t, int32(3), attempts.Load())
			assert.Equal(t, []time.Duration{callbackRetryInitialDelay, callbackRetryInitialDelay * 2}, sleepDurations)
			select {
			case output := <-outputCh:
				assert.Equal(t, expectedOutput, output)
			default:
				t.Fatal("callback not called")
			}
		})
	})

	t.Run("forwards execution error to callback", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			inputData := []byte("test input")
			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, inputData)

			expectedErr := errors.New("execution failed")
			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					return nil, expectedErr
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			callbackErr := make(chan error, 1)
			callback := func(_ context.Context, output []byte, err error) error {
				callbackErr <- err
				return nil
			}

			err = runnable.Run(t.Context(), t.Name(), callback)
			assert.NoError(t, err)
			select {
			case receivedErr := <-callbackErr:
				assert.ErrorIs(t, receivedErr, expectedErr)
			default:
				t.Fatal("callback not called")
			}
		})
	})

	t.Run("restarts execution when input changes", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			initialInput := []byte("initial")
			setInput(t, db, tkey, initialInput)

			executionCount := atomic.Int32{}
			inputReceived := make(chan []byte, 10)
			executionStarted := make(chan struct{}, 10)
			continueExecution := make(chan struct{})

			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					count := executionCount.Add(1)
					inputReceived <- marshalledInput
					executionStarted <- struct{}{}

					// First execution blocks until cancelled or signaled
					if count == 1 {
						select {
						case <-ctx.Done():
							return nil, context.Cause(ctx)
						case <-continueExecution:
							return []byte("first output"), nil
						}
					}

					// Second execution completes immediately
					return []byte("second output"), nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			outputCh := make(chan []byte, 1)
			callback := func(_ context.Context, output []byte, err error) error {
				assert.NoError(t, err)
				outputCh <- output
				return nil
			}

			var runErr error
			done := make(chan struct{})
			go func() {
				runErr = runnable.Run(t.Context(), t.Name(), callback)
				close(done)
			}()

			// Wait for first execution to start
			select {
			case <-executionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first execution to start")
			}

			// Update the input while first execution is running
			newInput := []byte("updated")
			setInput(t, db, tkey, newInput)

			// Wait for second execution to start
			select {
			case <-executionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for second execution to start")
			}

			// Wait for Run to complete
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for Run to complete")
			}

			assert.NoError(t, runErr)
			select {
			case output := <-outputCh:
				assert.Equal(t, []byte("second output"), output)
			default:
				t.Fatal("callback not called")
			}
			assert.Equal(t, int32(2), executionCount.Load())
		})
	})

	t.Run("cancelled execution due to input change returns ErrInputChanged", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			initialInput := []byte("initial")
			setInput(t, db, tkey, initialInput)

			firstExecutionErr := make(chan error, 1)
			executionStarted := make(chan struct{}, 10)
			executionCount := atomic.Int32{}

			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					count := executionCount.Add(1)
					executionStarted <- struct{}{}

					// Block until context is cancelled
					<-ctx.Done()
					err := context.Cause(ctx)

					// Only capture the first execution's error
					if count == 1 {
						firstExecutionErr <- err
					}
					return nil, err
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go func() {
				runnable.Run(ctx, t.Name(), func(context.Context, []byte, error) error { return nil })
			}()

			// Wait for first execution to start
			select {
			case <-executionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first execution to start")
			}

			// Update the input to trigger cancellation of first execution
			setInput(t, db, tkey, []byte("updated"))

			// Wait for second execution to start (confirming first was cancelled)
			select {
			case <-executionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for second execution to start")
			}

			// Check the first execution's error (should be ErrInputChanged)
			select {
			case err := <-firstExecutionErr:
				assert.ErrorIs(t, err, errInputChanged)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first execution error")
			}
		})
	})

	t.Run("context cancellation stops execution", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			inputData := []byte("test input")
			setInput(t, db, tkey, inputData)

			executionStarted := make(chan struct{})
			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					close(executionStarted)
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			ctx, cancel := context.WithCancel(t.Context())

			done := make(chan struct{})
			var runErr error
			callbackErr := make(chan error, 1)
			go func() {
				runErr = runnable.Run(ctx, t.Name(), func(_ context.Context, _ []byte, err error) error {
					callbackErr <- err
					return nil
				})
				close(done)
			}()

			// Wait for execution to start
			select {
			case <-executionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for execution to start")
			}

			// Cancel the context
			cancel()

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for Run to complete")
			}

			assert.NoError(t, runErr)
			select {
			case err := <-callbackErr:
				assert.ErrorIs(t, err, context.Canceled)
			default:
				t.Fatal("callback not called")
			}
		})
	})
	t.Run("waits for execution to exit before returning", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("test input"))

			started := make(chan struct{}, 1)
			release := make(chan struct{})
			exited := make(chan struct{}, 1)
			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					started <- struct{}{}
					<-release
					exited <- struct{}{}
					return nil, nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			done := make(chan struct{})
			var runErr error
			go func() {
				runErr = runnable.Run(t.Context(), t.Name(), func(_ context.Context, _ []byte, err error) error {
					assert.NoError(t, err)
					return nil
				})
				close(done)
			}()

			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for execution to start")
			}

			select {
			case <-done:
				t.Fatal("Run returned before execution finished")
			case <-time.After(100 * time.Millisecond):
			}

			close(release)

			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for execution to exit")
			}

			select {
			case <-done:
				assert.NoError(t, runErr)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for Run to return")
			}
		})
	})

	t.Run("execution receives correct input", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			expectedInput := []byte("expected input data")
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, expectedInput)

			var receivedInput []byte
			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					receivedInput = marshalledInput
					return []byte("output"), nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			err = runnable.Run(t.Context(), t.Name(), func(_ context.Context, _ []byte, _ error) error { return nil })
			assert.NoError(t, err)

			// The Watch function decodes with privateencoding, so receivedInput is already the decoded value
			assert.Equal(t, expectedInput, receivedInput)
		})
	})

	t.Run("multiple rapid input changes eventually complete with latest", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("initial"))

			executionCount := atomic.Int32{}

			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					count := executionCount.Add(1)
					_ = marshalledInput // used to track input changes

					// Earlier executions wait to be cancelled
					if count < 5 {
						select {
						case <-ctx.Done():
							return nil, context.Cause(ctx)
						case <-time.After(5 * time.Second):
							return []byte(fmt.Sprintf("output-%d", count)), nil
						}
					}

					// Final execution completes immediately
					return []byte("final-output"), nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			done := make(chan struct{})
			outputCh := make(chan []byte, 1)
			callback := func(_ context.Context, output []byte, err error) error {
				assert.NoError(t, err)
				outputCh <- output
				return nil
			}

			var runErr error
			go func() {
				runErr = runnable.Run(t.Context(), t.Name(), callback)
				close(done)
			}()

			// Rapid fire input changes
			for i := range 5 {
				time.Sleep(50 * time.Millisecond)
				setInput(t, db, tkey, []byte(fmt.Sprintf("input-%d", i)))
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for Run to complete")
			}

			assert.NoError(t, runErr)
			select {
			case output := <-outputCh:
				assert.Equal(t, []byte("final-output"), output)
			default:
				t.Fatal("callback not called")
			}
		})
	})

	t.Run("returns ErrRunFatal when execution encounters an error", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.TaskKey(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("test input"))

			executor := &testutil.MockExecutor{
				Execute: func(
					inContainer executiontype.TransactionalContainer,
					ctx context.Context,
					marshalledInput []byte,
					opts ...ftype.FlowLoopOption,
				) ([]byte, error) {
					return nil, ErrRunFatal
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			err = runnable.Run(t.Context(), t.Name(), func(_ context.Context, _ []byte, _ error) error { return nil })
			assert.ErrorIs(t, err, ErrRunFatal)
		})
	})
}
