package pool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/run"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
)

func setInput(t *testing.T, db dbutil.DbRoot, tkey task.TaskKey, input []byte) {
	t.Helper()
	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		tkey.Input().Set(tx, input)
		return nil, nil
	})
	assert.NoError(t, err)
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
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Execute: func(inContainer executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
						return []byte("output"), nil
					},
				},
				db.Database,
				tkey,
				executiontype.NewInMemoryContainer(),
			)
			var wg sync.WaitGroup
			wg.Add(1)
			err = m.run(t.Context(), runnable, func(_ context.Context, output []byte, err error) error {
				// it should not be cleaned up when until callback exits successfully
				assert.Equal(t, 1, len(m.runCancels))
				assert.Equal(t, []byte("output"), output)
				assert.NoError(t, err)
				wg.Done()
				return nil
			})
			assert.NoError(t, err)
			wg.Wait()

			// give some time for the cleanup to complete
			time.Sleep(100 * time.Millisecond)
			assert.Equal(t, 0, len(m.runCancels))
		})
	})

	t.Run("run, then cancel", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))

			var executeWg sync.WaitGroup
			executeWg.Add(1)
			var callbackWg sync.WaitGroup
			callbackWg.Add(1)
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			id := task.NewId()
			tkey, err := tasksDirectory.Create(db, id)
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Execute: func(inContainer executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
						executeWg.Done()
						<-ctx.Done()
						return []byte("output"), ctx.Err()
					},
				},
				db.Database,
				tkey,
				executiontype.NewInMemoryContainer(),
			)
			err = m.run(t.Context(), runnable, func(_ context.Context, output []byte, err error) error {
				assert.Equal(t, []byte("output"), output)
				assert.ErrorIs(t, err, context.Canceled)
				callbackWg.Done()
				return nil
			})
			assert.NoError(t, err)

			executeWg.Wait()
			err = m.cancel(runnable.Id())
			assert.NoError(t, err)

			t.Run("duplicate cancel should return non existent run error", func(t *testing.T) {
				err := m.cancel(runnable.Id())
				assert.ErrorIs(t, err, ErrRunNotFound)
			})

			callbackWg.Wait()
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
			var callbackWg sync.WaitGroup
			callbackWg.Add(runCount)
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			for range runCount {
				tkey, err := tasksDirectory.Create(db, task.NewId())
				assert.NoError(t, err)
				setInput(t, db, tkey, []byte("input"))
				err = m.run(ctx, run.NewRunnable(
					&testutil.MockExecutor{
						Execute: func(inContainer executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
							executeWg.Done()
							<-ctx.Done()
							return nil, ctx.Err()
						},
					},
					db.Database,
					tkey,
					executiontype.NewInMemoryContainer(),
				), func(_ context.Context, output []byte, err error) error {
					assert.Nil(t, output)
					assert.ErrorIs(t, err, context.Canceled)
					callbackWg.Done()
					return nil
				})
				assert.NoError(t, err)
			}
			assert.Equal(t, runCount, len(m.runCancels))
			executeWg.Wait()

			cancel()
			m.wait()
			callbackWg.Wait()

			assert.Equal(t, 0, len(m.runCancels))
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
			var callbackWg sync.WaitGroup
			callbackWg.Add(runCount)
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			for range runCount {
				tkey, err := tasksDirectory.Create(db, task.NewId())
				assert.NoError(t, err)
				setInput(t, db, tkey, []byte("input"))
				err = m.run(ctx, run.NewRunnable(
					&testutil.MockExecutor{
						Execute: func(inContainer executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
							executeWg.Done()
							<-ctx.Done()
							time.Sleep(sleepTime)
							return nil, ctx.Err()
						},
					},
					db.Database,
					tkey,
					executiontype.NewInMemoryContainer(),
				), func(_ context.Context, output []byte, err error) error {
					assert.Nil(t, output)
					assert.ErrorIs(t, err, context.Canceled)
					callbackWg.Done()
					return nil
				})
				assert.NoError(t, err)
			}
			executeWg.Wait()

			start := time.Now()
			cancel()
			m.wait()
			elapsed := time.Since(start)
			assert.GreaterOrEqual(t, elapsed, sleepTime)

			callbackWg.Wait()
			assert.Equal(t, 0, len(m.runCancels))
		})
	})
	t.Run("execution error passed to callback", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			m := newRunMap(t.Name(), neverCallErrorCallback(t))
			expectedErr := fmt.Errorf("execution failed")
			var wg sync.WaitGroup
			wg.Add(1)
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			assert.NoError(t, err)
			tkey, err := tasksDirectory.Create(db, task.NewId())
			assert.NoError(t, err)
			setInput(t, db, tkey, []byte("input"))
			runnable := run.NewRunnable(
				&testutil.MockExecutor{
					Execute: func(inContainer executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
						return nil, expectedErr
					},
				},
				db.Database,
				tkey,
				executiontype.NewInMemoryContainer(),
			)
			err = m.run(t.Context(), runnable, func(_ context.Context, output []byte, err error) error {
				assert.Nil(t, output)
				assert.ErrorIs(t, err, expectedErr)
				wg.Done()
				return nil
			})
			assert.NoError(t, err)
			wg.Wait()
		})
	})
	t.Run("wait on empty map", func(t *testing.T) {
		m := newRunMap(t.Name(), neverCallErrorCallback(t))
		m.wait() // should not panic
		assert.Equal(t, 0, len(m.runCancels))
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
					Execute: func(inContainer executiontype.TransactionalContainer, ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
						return nil, nil
					},
				},
				db.Database,
				tkey,
				executiontype.NewInMemoryContainer(),
			)
			err = m.run(t.Context(), runnable, func(_ context.Context, b []byte, err error) error {
				return nil
			})
			assert.NoError(t, err)
			err = m.run(t.Context(), runnable, func(_ context.Context, b []byte, err error) error {
				return nil
			})
			assert.ErrorIs(t, err, ErrDuplicateRun)
		})
	})
}
