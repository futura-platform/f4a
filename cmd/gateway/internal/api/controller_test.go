package api

import (
	"context"
	"errors"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
)

func newTestController(t *testing.T, db dbutil.DbRoot) *controller {
	t.Helper()

	handler, err := NewController(db)
	require.NoError(t, err)

	c, ok := handler.(*controller)
	require.True(t, ok)
	return c
}

type taskState struct {
	ExecutorID      string
	CallbackURL     string
	Input           []byte
	LifecycleStatus task.LifecycleStatus
}

func mustCreateTask(t *testing.T, c *controller, taskID, executorID, callbackURL string, input []byte) {
	t.Helper()
	_, err := c.CreateTask(context.Background(), &taskv1.CreateTaskRequest{
		TaskId:      taskID,
		ExecutorId:  executorID,
		CallbackUrl: callbackURL,
		Parameters:  &taskv1.TaskParameters{Input: input},
	})
	require.NoError(t, err)
}

func mustUpdateTask(t *testing.T, c *controller, taskID string, input []byte) {
	t.Helper()
	_, err := c.UpdateTask(context.Background(), &taskv1.UpdateTaskRequest{
		TaskId:     taskID,
		Parameters: &taskv1.TaskParameters{Input: input},
	})
	require.NoError(t, err)
}

func mustActivateTask(t *testing.T, c *controller, taskID string) {
	t.Helper()
	_, err := c.ActivateTask(context.Background(), &taskv1.ActivateTaskRequest{
		TaskId: taskID,
	})
	require.NoError(t, err)
}

func mustSuspendTask(t *testing.T, c *controller, taskID string) {
	t.Helper()
	_, err := c.SuspendTask(context.Background(), &taskv1.SuspendTaskRequest{
		TaskId: taskID,
	})
	require.NoError(t, err)
}

func mustDeleteTask(t *testing.T, c *controller, taskID string) {
	t.Helper()
	_, err := c.DeleteTask(context.Background(), &taskv1.DeleteTaskRequest{
		TaskId: taskID,
	})
	require.NoError(t, err)
}

func readTaskState(t *testing.T, db dbutil.DbRoot, taskID string) (taskState, bool) {
	t.Helper()

	taskDir, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	var (
		state  taskState
		exists bool
	)
	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		tkey, err := taskDir.Open(tx, task.Id(taskID))
		if err != nil {
			if errors.Is(err, directory.ErrDirNotExists) {
				return nil, nil
			}
			return nil, err
		}

		exists = true
		state = taskState{
			ExecutorID:      string(tkey.ExecutorId().Get(tx).MustGet()),
			CallbackURL:     tkey.CallbackUrl().Get(tx).MustGet(),
			Input:           tkey.Input().Get(tx).MustGet(),
			LifecycleStatus: tkey.LifecycleStatus().Get(tx).MustGet(),
		}
		return nil, nil
	})
	require.NoError(t, err)
	return state, exists
}

func setTaskLifecycleStatus(t *testing.T, db dbutil.DbRoot, taskID string, status task.LifecycleStatus) {
	t.Helper()

	taskDir, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		tkey, err := taskDir.Open(tx, task.Id(taskID))
		if err != nil {
			return nil, err
		}
		tkey.LifecycleStatus().Set(tx, status)
		return nil, nil
	})
	require.NoError(t, err)
}

// Note: nested "idempotency" subtests intentionally reuse state established by
// the "functionality" parent subtest. Keep these subtests sequential
// (do not call t.Parallel) so ordering stays deterministic.
func TestControllerCreateTask(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("functionality", func(t *testing.T) {
			// Intentionally sequential: idempotency subtest reuses this state.
			taskID := "create-functional"
			expected := taskState{
				ExecutorID:      "executor-a",
				CallbackURL:     "https://example.com/a",
				Input:           []byte("payload-a"),
				LifecycleStatus: task.LifecycleStatusSuspended,
			}
			mustCreateTask(t, c, taskID, expected.ExecutorID, expected.CallbackURL, expected.Input)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, expected, state)

			t.Run("idempotency", func(t *testing.T) {
				// Replay with different values should be a no-op.
				mustCreateTask(t, c, taskID, "executor-new", "https://example.com/new", []byte("new"))

				state, exists := readTaskState(t, db, taskID)
				require.True(t, exists)
				require.Equal(t, expected, state)
			})
		})
	})
}

func TestControllerUpdateTask(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("functionality", func(t *testing.T) {
			taskID := "update-functional"
			mustCreateTask(t, c, taskID, "executor-a", "https://example.com/a", []byte("before"))
			mustUpdateTask(t, c, taskID, []byte("after"))

			expected, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, []byte("after"), expected.Input)
			require.Equal(t, "executor-a", expected.ExecutorID)
			require.Equal(t, "https://example.com/a", expected.CallbackURL)
			require.Equal(t, task.LifecycleStatusSuspended, expected.LifecycleStatus)

			t.Run("idempotency", func(t *testing.T) {
				mustUpdateTask(t, c, taskID, []byte("after"))

				state, exists := readTaskState(t, db, taskID)
				require.True(t, exists)
				require.Equal(t, expected, state)
			})
		})
	})
}

func TestControllerActivateTask(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("functionality", func(t *testing.T) {
			taskID := "activate-functional"
			mustCreateTask(t, c, taskID, "executor-a", "https://example.com/a", []byte("payload"))
			mustActivateTask(t, c, taskID)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, task.LifecycleStatusPending, state.LifecycleStatus)

			t.Run("idempotency", func(t *testing.T) {
				mustActivateTask(t, c, taskID)

				state, exists := readTaskState(t, db, taskID)
				require.True(t, exists)
				require.Equal(t, task.LifecycleStatusPending, state.LifecycleStatus)
			})
		})
	})
}

func TestControllerSuspendTask(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("functionality", func(t *testing.T) {
			taskID := "suspend-functional"
			mustCreateTask(t, c, taskID, "executor-a", "https://example.com/a", []byte("payload"))
			mustActivateTask(t, c, taskID)
			mustSuspendTask(t, c, taskID)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, task.LifecycleStatusSuspended, state.LifecycleStatus)

			t.Run("idempotency", func(t *testing.T) {
				mustSuspendTask(t, c, taskID)

				state, exists := readTaskState(t, db, taskID)
				require.True(t, exists)
				require.Equal(t, task.LifecycleStatusSuspended, state.LifecycleStatus)
			})
		})
	})
}

func TestControllerDeleteTask(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("functionality", func(t *testing.T) {
			taskID := "delete-functional"
			mustCreateTask(t, c, taskID, "executor-a", "https://example.com/a", []byte("payload"))
			mustActivateTask(t, c, taskID)
			mustDeleteTask(t, c, taskID)

			_, exists := readTaskState(t, db, taskID)
			require.False(t, exists)

			t.Run("idempotency", func(t *testing.T) {
				// Repeated delete on the same task should be a no-op.
				mustDeleteTask(t, c, taskID)
				_, exists := readTaskState(t, db, taskID)
				require.False(t, exists)

				// Deleting an unknown task should also be a no-op.
				mustDeleteTask(t, c, "delete-missing")
			})
		})
	})
}

func TestControllerActivateTask_RunningTaskIsNoOp(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)
		taskID := "activate-running-noop"

		mustCreateTask(t, c, taskID, "executor-a", "https://example.com/a", []byte("payload"))
		setTaskLifecycleStatus(t, db, taskID, task.LifecycleStatusRunning)
		mustActivateTask(t, c, taskID)

		state, exists := readTaskState(t, db, taskID)
		require.True(t, exists)
		require.Equal(t, task.LifecycleStatusRunning, state.LifecycleStatus)
	})
}
