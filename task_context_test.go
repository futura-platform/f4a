package f4a

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskIdFromContext_Integration(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		require.NoError(t, db.Options().SetTransactionRetryLimit(3))

		runnerID := "test-runner"
		executorID := execute.ExecutorId("test-executor")
		taskID := task.NewId()

		tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)

		taskKey, err := tasksDirectory.Create(db, taskID)
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			taskKey.ExecutorId().Set(tx, executorID)
			taskKey.Input().Set(tx, []byte("input"))
			taskKey.RunnerId().Set(tx, &runnerID)
			taskKey.LifecycleStatus().Set(tx, task.LifecycleStatusRunning)
			return nil, nil
		})
		require.NoError(t, err)

		taskSet, err := pool.CreateOrOpenTaskSetForRunner(db, db, runnerID)
		require.NoError(t, err)

		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			return nil, taskSet.Add(tx, []byte(taskID))
		})
		require.NoError(t, err)

		type executionContextResult struct {
			taskID string
			ok     bool
		}

		executionContextCh := make(chan executionContextResult, 1)
		runErrCh := make(chan error, 1)

		router := execute.NewRouter(execute.Route{
			Id: executorID,
			Executor: &testutil.MockExecutor{
				Execute: func(
					_ executiontype.TransactionalContainer,
					ctx context.Context,
					_ []byte,
					_ ...ftype.FlowLoopOption,
				) ([]byte, error) {
					id, ok := TaskIdFromContext(ctx)
					executionContextCh <- executionContextResult{
						taskID: id,
						ok:     ok,
					}
					return []byte("output"), nil
				},
			},
		})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		go func() {
			runErrCh <- pool.RunWorkLoop(ctx, runnerID, db, taskSet, router)
		}()

		select {
		case result := <-executionContextCh:
			assert.True(t, result.ok)
			assert.Equal(t, string(taskID), result.taskID)
		case err := <-runErrCh:
			t.Fatalf("work loop exited early: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for task execution")
		}

		waitForTaskDeletion(t, db, taskID)

		cancel()
		select {
		case err := <-runErrCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for work loop shutdown")
		}
	})
}

func waitForTaskDeletion(t testing.TB, db dbutil.DbRoot, id task.Id) {
	t.Helper()

	tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		exists := false
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			_, err := tasksDirectory.Open(tx, id)
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					return nil, nil
				}
				return nil, err
			}
			exists = true
			return nil, nil
		})
		require.NoError(t, err)
		if !exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for task deletion: %s", id)
}
