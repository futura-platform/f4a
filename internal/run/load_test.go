package run

import (
	"context"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/fdbexec"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/internal/util"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setTaskMetadata(
	t *testing.T,
	db util.DbRoot,
	taskKey task.TaskKey,
	executorId *execute.ExecutorId,
	callbackUrl *string,
) {
	t.Helper()

	_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
		if executorId != nil {
			taskKey.ExecutorId().Set(tx, *executorId)
		}
		if callbackUrl != nil {
			taskKey.CallbackUrl().Set(tx, *callbackUrl)
		}
		return nil, nil
	})
	require.NoError(t, err)
}

func TestLoadTasks(t *testing.T) {
	t.Run("loads tasks with executor and callback", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			require.NoError(t, err)

			idOne := task.NewId()
			idTwo := task.NewId()
			executorIdOne := execute.ExecutorId("executor-1")
			executorIdTwo := execute.ExecutorId("executor-2")
			callbackUrlOne := "https://example.com/callback-1"
			callbackUrlTwo := "https://example.com/callback-2"

			tkeyOne, err := tasksDirectory.Create(db, idOne)
			assert.NoError(t, err)
			tkeyTwo, err := tasksDirectory.Create(db, idTwo)
			assert.NoError(t, err)
			setTaskMetadata(t, db, tkeyOne, &executorIdOne, &callbackUrlOne)
			setTaskMetadata(t, db, tkeyTwo, &executorIdTwo, &callbackUrlTwo)

			executorOne := &testutil.MockExecutor{Execute: func(_ executiontype.TransactionalContainer, _ context.Context, _ []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
				return nil, nil
			}}
			executorTwo := &testutil.MockExecutor{Execute: func(_ executiontype.TransactionalContainer, _ context.Context, _ []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
				return nil, nil
			}}
			router := execute.NewRouter(
				execute.Route{Id: executorIdOne, Executor: executorOne},
				execute.Route{Id: executorIdTwo, Executor: executorTwo},
			)

			loadedTasks, err := LoadTasks(t.Context(), db, router, []task.Id{idOne, idTwo})
			require.NoError(t, err)
			assert.Len(t, loadedTasks, 2)

			loadedById := make(map[task.Id]RunnableTask, len(loadedTasks))
			for _, loaded := range loadedTasks {
				loadedById[loaded.Id()] = loaded
			}

			require.Contains(t, loadedById, idOne)
			loadedOne := loadedById[idOne]
			assert.Equal(t, callbackUrlOne, loadedOne.CallbackUrl().String())
			assert.Same(t, executorOne, loadedOne.Runnable.executor)
			assert.Equal(t, idOne, loadedOne.Id())
			assert.IsType(t, &fdbexec.ExecutionContainer{}, loadedOne.execution)

			require.Contains(t, loadedById, idTwo)
			loadedTwo := loadedById[idTwo]
			assert.Equal(t, callbackUrlTwo, loadedTwo.CallbackUrl().String())
			assert.Same(t, executorTwo, loadedTwo.executor)
			assert.Equal(t, idTwo, loadedTwo.Id())
			assert.IsType(t, &fdbexec.ExecutionContainer{}, loadedTwo.execution)
		})
	})

	t.Run("returns error when executor id missing", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			require.NoError(t, err)

			id := task.NewId()
			callbackUrl := "https://example.com/callback"
			tkey, err := tasksDirectory.Create(db, id)
			assert.NoError(t, err)
			setTaskMetadata(t, db, tkey, nil, &callbackUrl)

			_, err = LoadTasks(t.Context(), db, execute.NewRouter(), []task.Id{id})
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed to get executor id")
		})
	})

	t.Run("returns error when executor cannot be routed", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			require.NoError(t, err)

			id := task.NewId()
			executorId := execute.ExecutorId("missing-executor")
			callbackUrl := "https://example.com/callback"
			tkey, err := tasksDirectory.Create(db, id)
			assert.NoError(t, err)
			setTaskMetadata(t, db, tkey, &executorId, &callbackUrl)

			_, err = LoadTasks(t.Context(), db, execute.NewRouter(), []task.Id{id})
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed to route executor")
		})
	})

	t.Run("returns error when callback url is invalid", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			tasksDirectory, err := task.CreateOrOpenTasksDirectory(db)
			require.NoError(t, err)

			id := task.NewId()
			executorId := execute.ExecutorId("executor-1")
			callbackUrl := "http://example.com/%zz"
			tkey, err := tasksDirectory.Create(db, id)
			assert.NoError(t, err)
			setTaskMetadata(t, db, tkey, &executorId, &callbackUrl)

			executor := &testutil.MockExecutor{Execute: func(_ executiontype.TransactionalContainer, _ context.Context, _ []byte, _ ...ftype.FlowLoopOption) ([]byte, error) {
				return nil, nil
			}}
			router := execute.NewRouter(execute.Route{Id: executorId, Executor: executor})

			_, err = LoadTasks(t.Context(), db, router, []task.Id{id})
			assert.Error(t, err)
			assert.ErrorContains(t, err, "failed to parse callback url")
		})
	})
}
