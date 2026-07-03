package pool

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setInput(t *testing.T, db dbutil.DbRoot, tkey task.TaskKey, input []byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		tkey.Input().Set(tx, input)
		return nil, nil
	})
	assert.NoError(t, err)
}

// testCallbackUrl returns a syntactically valid callback url; runs backed by
// the mock executor never dial it (delivery lives inside Settle).
func testCallbackUrl(t testing.TB) *url.URL {
	t.Helper()
	u, err := url.Parse("http://127.0.0.1:1/test-callback")
	require.NoError(t, err)
	return u
}

func neverCallErrorCallback(t testing.TB) func(id task.Id, err error) {
	t.Helper()
	return func(id task.Id, err error) {
		t.Errorf("error callback called for run %s: %v", id, err)
	}
}

func TestRunMap(t *testing.T) {
	t.Run("natural run completion", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))
			var settleWg sync.WaitGroup
			settleWg.Add(1)
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
						// the run state must not be cleaned up while settlement is in flight
						m.mu.Lock()
						assert.Equal(t, 1, len(m.runStates))
						m.mu.Unlock()
						settleWg.Done()
						return nil
					},
				},
				execute.ExecutorId("test"),
				db,
				tkey,
			)
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)
			settleWg.Wait()

			// the run state is cleaned up once the run completes
			assert.Eventually(t, func() bool {
				m.mu.Lock()
				defer m.mu.Unlock()
				return len(m.runStates) == 0
			}, 2*time.Second, 10*time.Millisecond)
		})
	})

	t.Run("run, then cancel", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))

			var executeWg sync.WaitGroup
			executeWg.Add(1)
			canceledCh := make(chan error, 1)
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			id := task.NewId()
			tkey, err := tasksDirectory.Create(db, id)
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
						executeWg.Done()
						<-ctx.Done()
						canceledCh <- context.Cause(ctx)
						return ctx.Err()
					},
				},
				execute.ExecutorId("test"),
				db,
				tkey,
			)
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)

			executeWg.Wait()
			err = m.cancel(runnable.Id())
			assert.NoError(t, err)

			select {
			case cause := <-canceledCh:
				assert.ErrorIs(t, cause, context.Canceled)
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for settlement to observe cancellation")
			}

			t.Run("duplicate cancel should return non existent run error after cleanup", func(t *testing.T) {
				if !assert.Eventually(t, func() bool {
					m.mu.Lock()
					defer m.mu.Unlock()
					_, ok := m.runStates[runnable.Id()]
					return !ok
				}, 2*time.Second, 10*time.Millisecond) {
					return
				}

				err := m.cancel(runnable.Id())
				assert.ErrorIs(t, err, ErrRunNotFound)
			})
		})
	})
	t.Run("run, then cancel parent context", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			runCount := 10
			var executeWg sync.WaitGroup
			executeWg.Add(runCount)
			var canceledCount atomic.Int32
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			for range runCount {
				tkey, err := tasksDirectory.Create(db, task.NewId())
				assert.NoError(t, err)
				setInput(t, db, tkey, []byte("input"))
				err = m.run(ctx, run.NewRunnable(
					&testutil.MockExecutor{
						Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
							executeWg.Done()
							<-ctx.Done()
							canceledCount.Add(1)
							return ctx.Err()
						},
					},
					execute.ExecutorId("test"),
					db,
					tkey,
				), testCallbackUrl(t))
				assert.NoError(t, err)
			}
			assert.Equal(t, runCount, len(m.runStates))
			executeWg.Wait()

			cancel()
			m.wait()

			assert.Equal(t, int32(runCount), canceledCount.Load())
			assert.Equal(t, 0, len(m.runStates))
		})
	})
	t.Run("wait blocks until runs exit after parent cancellation", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			runCount := 3
			sleepTime := 1 * time.Second
			var executeWg sync.WaitGroup
			executeWg.Add(runCount)
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			for range runCount {
				tkey, err := tasksDirectory.Create(db, task.NewId())
				assert.NoError(t, err)
				setInput(t, db, tkey, []byte("input"))
				err = m.run(ctx, run.NewRunnable(
					&testutil.MockExecutor{
						Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
							executeWg.Done()
							<-ctx.Done()
							time.Sleep(sleepTime)
							return ctx.Err()
						},
					},
					execute.ExecutorId("test"),
					db,
					tkey,
				), testCallbackUrl(t))
				assert.NoError(t, err)
			}
			executeWg.Wait()

			start := time.Now()
			cancel()
			m.wait()
			elapsed := time.Since(start)
			assert.GreaterOrEqual(t, elapsed, sleepTime)

			assert.Equal(t, 0, len(m.runStates))
		})
	})
	t.Run("settle error is reported via onRunError", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			expectedErr := fmt.Errorf("settlement failed")
			type runError struct {
				id  task.Id
				err error
			}
			runErrCh := make(chan runError, 1)
			m := newRunMap(t.Name(), func(id task.Id, err error) {
				runErrCh <- runError{id: id, err: err}
			})
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
						return expectedErr
					},
				},
				execute.ExecutorId("test"),
				db,
				tkey,
			)
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)

			select {
			case reported := <-runErrCh:
				assert.Equal(t, runnable.Id(), reported.id)
				assert.ErrorIs(t, reported.err, expectedErr)
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for onRunError to be called")
			}
			m.wait()
		})
	})
	t.Run("wait on empty map", func(t *testing.T) {
		m := newRunMap(t.Name(), neverCallErrorCallback(t))
		m.wait() // should not panic
		assert.Equal(t, 0, len(m.runStates))
	})
	t.Run("delete non-existent run", func(t *testing.T) {
		m := newRunMap(t.Name(), neverCallErrorCallback(t))
		err := m.cancel(task.NewId())
		assert.ErrorIs(t, err, ErrRunNotFound)
	})
	t.Run("duplicate run", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
						<-ctx.Done()
						return ctx.Err()
					},
				},
				execute.ExecutorId("test"),
				db,
				tkey,
			)
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.ErrorIs(t, err, ErrDuplicateRun)

			assert.NoError(t, m.cancel(runnable.Id()))
			m.wait()
		})
	})
	t.Run("remove add remove does not start queued successor", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))

			var executionCount atomic.Int32
			started := make(chan struct{}, 2)
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
						executionCount.Add(1)
						started <- struct{}{}
						<-ctx.Done()
						return ctx.Err()
					},
				},
				execute.ExecutorId("test"),
				db,
				tkey,
			)

			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first run to start")
			}

			assert.NoError(t, m.cancel(runnable.Id()))
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)
			assert.NoError(t, m.cancel(runnable.Id()))

			m.wait()
			assert.Equal(t, int32(1), executionCount.Load())
		})
	})
	t.Run("remove add remove add only starts final successor once", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))

			var executionCount atomic.Int32
			started := make(chan struct{}, 3)
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Settle: func(_ execute.SettlementContainers, ctx context.Context, marshalledInput []byte, _ *url.URL, _ ...ftype.FlowLoopOption) error {
						executionCount.Add(1)
						started <- struct{}{}
						<-ctx.Done()
						return ctx.Err()
					},
				},
				execute.ExecutorId("test"),
				db,
				tkey,
			)

			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first run to start")
			}

			assert.NoError(t, m.cancel(runnable.Id()))
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)
			assert.NoError(t, m.cancel(runnable.Id()))
			err = m.run(t.Context(), runnable, testCallbackUrl(t))
			assert.NoError(t, err)

			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for final successor to start")
			}

			assert.NoError(t, m.cancel(runnable.Id()))
			m.wait()
			assert.Equal(t, int32(2), executionCount.Load())
		})
	})
}
