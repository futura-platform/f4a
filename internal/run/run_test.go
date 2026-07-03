package run

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rawStringMarshaller struct{}

func (rawStringMarshaller) UnmarshalInput(data []byte) (string, error) {
	return string(data), nil
}

func (rawStringMarshaller) MarshalOutput(data string) ([]byte, error) {
	return []byte(data), nil
}

func setInput(t *testing.T, db dbutil.DbRoot, td task.TaskKey, value []byte) {
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		td.Input().Set(tx, value)
		return nil, nil
	})
	assert.NoError(t, err)
}

// testCallbackUrl returns a syntactically valid callback url for tests that
// never actually deliver anything (Run requires a non-nil callback url, but
// mock executors do not dial it).
func testCallbackUrl(t testing.TB) *url.URL {
	t.Helper()
	u, err := url.Parse("http://127.0.0.1:1/test-callback")
	require.NoError(t, err)
	return u
}

func TestRun(t *testing.T) {
	t.Run("returns nil once the task settles", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("test input"))

			settleCalls := atomic.Int32{}
			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					_ context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					settleCalls.Add(1)
					return nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			err = runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
			assert.NoError(t, err)
			assert.Equal(t, int32(1), settleCalls.Load())
		})
	})

	t.Run("restarts execution when input changes", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			initialInput := []byte("initial")
			setInput(t, db, tkey, initialInput)

			executionCount := atomic.Int32{}
			inputReceived := make(chan []byte, 10)
			executionStarted := make(chan struct{}, 10)
			continueExecution := make(chan struct{})

			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					ctx context.Context,
					marshalledInput []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					count := executionCount.Add(1)
					inputReceived <- marshalledInput
					executionStarted <- struct{}{}

					// First execution blocks until cancelled or signaled
					if count == 1 {
						select {
						case <-ctx.Done():
							return context.Cause(ctx)
						case <-continueExecution:
							return nil
						}
					}

					// Second execution settles immediately
					return nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			var runErr error
			done := make(chan struct{})
			go func() {
				runErr = runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
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
			assert.Equal(t, int32(2), executionCount.Load())
			assert.Equal(t, initialInput, <-inputReceived)
			assert.Equal(t, newInput, <-inputReceived)
		})
	})

	t.Run("real futura executor replays input changes during delayed exit", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("initial"))

			deliveredCh := make(chan []byte, 2)
			server := testutil.NewEphemeralHTTPServer(t, func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				deliveredCh <- body
				w.WriteHeader(http.StatusAccepted)
			})
			callbackUrl, err := url.Parse(server.URL + "/callback")
			require.NoError(t, err)

			firstExecutionStarted := make(chan struct{})
			executionCount := atomic.Int32{}
			exitDelay := 200 * time.Millisecond

			executor := execute.NewExecutor(
				func(b futura.FlowBuilder, input string) (string, error) {
					if executionCount.Add(1) == 1 {
						close(firstExecutionStarted)
						<-b.Done()
						time.Sleep(exitDelay)
						return "", context.Cause(b)
					}
					return "output:" + input, nil
				},
				rawStringMarshaller{},
			)

			runnable := NewRunnable(executor, execute.ExecutorId("test-executor"), db, tkey)

			done := make(chan struct{})
			var runErr error
			go func() {
				runErr = runnable.Run(t.Context(), t.Name(), callbackUrl)
				close(done)
			}()

			select {
			case <-firstExecutionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first execution to start")
			}

			setInput(t, db, tkey, []byte("updated"))

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("timeout waiting for Run to complete")
			}

			assert.NoError(t, runErr)
			assert.Equal(t, int32(2), executionCount.Load())

			select {
			case body := <-deliveredCh:
				assert.Equal(t, []byte("output:updated"), body)
			default:
				t.Fatal("result was not delivered to the callback")
			}
		})
	})

	t.Run("cancelled execution due to input change returns ErrInputChanged", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			initialInput := []byte("initial")
			setInput(t, db, tkey, initialInput)

			firstExecutionErr := make(chan error, 1)
			executionStarted := make(chan struct{}, 10)
			executionCount := atomic.Int32{}

			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					ctx context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					count := executionCount.Add(1)
					executionStarted <- struct{}{}

					// Block until context is cancelled
					<-ctx.Done()
					err := context.Cause(ctx)

					// Only capture the first execution's error
					if count == 1 {
						firstExecutionErr <- err
					}
					return err
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			runErrCh := make(chan error, 1)
			go func() {
				runErrCh <- runnable.Run(ctx, t.Name(), testCallbackUrl(t))
			}()

			// Wait for first execution to start
			select {
			case <-executionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first execution to start")
			}

			// Update the input to trigger cancellation of first execution
			setInput(t, db, tkey, []byte("updated"))

			// Wait for second execution to start: this proves the input watch
			// survived the superseded execution (the regression tripwire for
			// the run.go fix).
			select {
			case <-executionStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for second execution to start")
			}

			// Check the first execution's context cause (should be errInputChanged)
			select {
			case err := <-firstExecutionErr:
				assert.ErrorIs(t, err, errInputChanged)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first execution error")
			}

			// The task never settles; aborting Run must surface the cancellation.
			cancel()
			select {
			case err := <-runErrCh:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for Run to return")
			}
		})
	})

	t.Run("context cancellation stops execution", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("test input"))

			executionStarted := make(chan struct{})
			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					ctx context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					close(executionStarted)
					<-ctx.Done()
					return context.Cause(ctx)
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
			go func() {
				runErr = runnable.Run(ctx, t.Name(), testCallbackUrl(t))
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

			// The task did not settle, so Run must NOT return nil.
			assert.ErrorIs(t, runErr, context.Canceled)
		})
	})

	t.Run("waits for execution to exit before returning", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("test input"))

			started := make(chan struct{}, 1)
			release := make(chan struct{})
			exited := make(chan struct{}, 1)
			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					_ context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					started <- struct{}{}
					<-release
					exited <- struct{}{}
					return nil
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
				runErr = runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
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

	t.Run("input changes after execution returns do not replay the completed execution", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			initialInput := []byte("initial")
			updatedInput := []byte("updated")
			setInput(t, db, tkey, initialInput)

			executionCount := atomic.Int32{}

			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					_ context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					if executionCount.Add(1) == 1 {
						// Commit a newer input while settlement completes: the
						// watch must not replay the already-settled execution.
						setInput(t, db, tkey, updatedInput)
					}
					return nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			err = runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
			assert.NoError(t, err)
			assert.Equal(t, int32(1), executionCount.Load())
		})
	})

	t.Run("execution receives correct input", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			expectedInput := []byte("expected input data")
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, expectedInput)

			var receivedInput []byte
			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					_ context.Context,
					marshalledInput []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					receivedInput = marshalledInput
					return nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			err = runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
			assert.NoError(t, err)

			// The Watch function decodes with privateencoding, so receivedInput is already the decoded value
			assert.Equal(t, expectedInput, receivedInput)
		})
	})

	t.Run("a superseded execution's outcome is not Run's return value", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)

			setInput(t, db, tkey, []byte("input"))

			supersededErr := errors.New("superseded execution outcome")
			executionStarted := make(chan struct{})
			executions := atomic.Int32{}
			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					ctx context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					if executions.Add(1) == 1 {
						close(executionStarted)
						// wait for the context to be cancelled on the first execution
						// (the input change should trigger this),
						// then fail with an outcome that must be discarded.
						<-ctx.Done()
						return supersededErr
					}
					return nil
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			runErrCh := make(chan error, 1)
			go func() {
				runErrCh <- runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
			}()

			// once execution starts, we should trigger an input change
			select {
			case runErr := <-runErrCh:
				t.Fatal("Run returned before execution started", runErr)
			case <-executionStarted:
			}
			setInput(t, db, tkey, []byte("updated"))

			select {
			case runErr := <-runErrCh:
				// only the replacement execution's outcome may be propagated
				assert.NotErrorIs(t, runErr, supersededErr)
				assert.NoError(t, runErr)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for Run to return")
			}

			assert.Equal(t, int32(2), executions.Load())
		})
	})

	t.Run("multiple rapid input changes eventually complete with latest", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("initial"))

			executionCount := atomic.Int32{}

			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					ctx context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					count := executionCount.Add(1)

					// Earlier executions wait to be superseded
					if count < 5 {
						select {
						case <-ctx.Done():
							return context.Cause(ctx)
						case <-time.After(5 * time.Second):
							return fmt.Errorf("execution %d was never superseded", count)
						}
					}

					// Final execution settles immediately
					return nil
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
				runErr = runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
				close(done)
			}()

			// Rapid fire input changes
			for i := range 5 {
				time.Sleep(50 * time.Millisecond)
				setInput(t, db, tkey, fmt.Appendf([]byte{}, "input-%d", i))
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for Run to complete")
			}

			assert.NoError(t, runErr)
			assert.GreaterOrEqual(t, executionCount.Load(), int32(5))
		})
	})

	t.Run("returns ErrRunFatal when execution encounters an error", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)

			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("test input"))

			executor := &testutil.MockExecutor{
				Settle: func(
					_ execute.SettlementContainers,
					_ context.Context,
					_ []byte,
					_ *url.URL,
					_ ...ftype.FlowLoopOption,
				) error {
					return ErrRunFatal
				},
			}

			runnable := Runnable{
				db:       db.Database,
				taskKey:  tkey,
				executor: executor,
			}

			err = runnable.Run(t.Context(), t.Name(), testCallbackUrl(t))
			assert.ErrorIs(t, err, ErrRunFatal)
		})
	})
}
