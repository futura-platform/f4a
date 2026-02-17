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

func mustCreateTask(
	t *testing.T,
	c *controller,
	revision uint64,
	taskID, executorID, callbackURL string,
	input []byte,
) {
	t.Helper()
	_, err := c.CreateTask(context.Background(), &taskv1.ControlServiceCreateTaskRequest{
		Revision: revision,
		Request: &taskv1.CreateTaskRequest{
			TaskId:      taskID,
			ExecutorId:  executorID,
			CallbackUrl: callbackURL,
			Parameters:  &taskv1.TaskParameters{Input: input},
		},
	})
	require.NoError(t, err)
}

func mustUpdateTask(t *testing.T, c *controller, revision uint64, taskID string, input []byte) {
	t.Helper()
	_, err := c.UpdateTask(context.Background(), &taskv1.ControlServiceUpdateTaskRequest{
		Revision: revision,
		Request: &taskv1.UpdateTaskRequest{
			TaskId:     taskID,
			Parameters: &taskv1.TaskParameters{Input: input},
		},
	})
	require.NoError(t, err)
}

func mustActivateTask(t *testing.T, c *controller, revision uint64, taskID string) {
	t.Helper()
	_, err := c.ActivateTask(context.Background(), &taskv1.ControlServiceActivateTaskRequest{
		Revision: revision,
		Request: &taskv1.ActivateTaskRequest{
			TaskId: taskID,
		},
	})
	require.NoError(t, err)
}

func mustSuspendTask(t *testing.T, c *controller, revision uint64, taskID string) {
	t.Helper()
	_, err := c.SuspendTask(context.Background(), &taskv1.ControlServiceSuspendTaskRequest{
		Revision: revision,
		Request: &taskv1.SuspendTaskRequest{
			TaskId: taskID,
		},
	})
	require.NoError(t, err)
}

func mustDeleteTask(t *testing.T, c *controller, revision uint64, taskID string) {
	t.Helper()
	_, err := c.DeleteTask(context.Background(), &taskv1.ControlServiceDeleteTaskRequest{
		Revision: revision,
		Request: &taskv1.DeleteTaskRequest{
			TaskId: taskID,
		},
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
			mustCreateTask(t, c, 1, taskID, expected.ExecutorID, expected.CallbackURL, expected.Input)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, expected, state)

			t.Run("idempotency", func(t *testing.T) {
				// Replay with different values should be a no-op.
				mustCreateTask(t, c, 1, taskID, "executor-new", "https://example.com/new", []byte("new"))

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
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("before"))
			mustUpdateTask(t, c, 2, taskID, []byte("after"))

			expected, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, []byte("after"), expected.Input)
			require.Equal(t, "executor-a", expected.ExecutorID)
			require.Equal(t, "https://example.com/a", expected.CallbackURL)
			require.Equal(t, task.LifecycleStatusSuspended, expected.LifecycleStatus)

			t.Run("idempotency", func(t *testing.T) {
				mustUpdateTask(t, c, 2, taskID, []byte("after"))

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
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))
			mustActivateTask(t, c, 2, taskID)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, task.LifecycleStatusPending, state.LifecycleStatus)

			t.Run("idempotency", func(t *testing.T) {
				mustActivateTask(t, c, 2, taskID)

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
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))
			mustActivateTask(t, c, 2, taskID)
			mustSuspendTask(t, c, 3, taskID)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, task.LifecycleStatusSuspended, state.LifecycleStatus)

			t.Run("idempotency", func(t *testing.T) {
				mustSuspendTask(t, c, 3, taskID)

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
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))
			mustActivateTask(t, c, 2, taskID)
			mustDeleteTask(t, c, 3, taskID)

			_, exists := readTaskState(t, db, taskID)
			require.False(t, exists)

			t.Run("idempotency", func(t *testing.T) {
				// Repeated delete on the same task should be a no-op.
				mustDeleteTask(t, c, 3, taskID)
				_, exists := readTaskState(t, db, taskID)
				require.False(t, exists)

				// Deleting an unknown task should also be a no-op.
				mustDeleteTask(t, c, 1, "delete-missing")
			})
		})
	})
}

func TestControllerActivateTask_RunningTaskIsNoOp(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)
		taskID := "activate-running-noop"

		mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))
		setTaskLifecycleStatus(t, db, taskID, task.LifecycleStatusRunning)
		mustActivateTask(t, c, 2, taskID)

		state, exists := readTaskState(t, db, taskID)
		require.True(t, exists)
		require.Equal(t, task.LifecycleStatusRunning, state.LifecycleStatus)
	})
}

func TestControllerRevisionRules(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("create revision must be one", func(t *testing.T) {
			_, err := c.CreateTask(context.Background(), &taskv1.ControlServiceCreateTaskRequest{
				Revision: 2,
				Request: &taskv1.CreateTaskRequest{
					TaskId:      "bad-create-revision",
					ExecutorId:  "executor-a",
					CallbackUrl: "https://example.com/a",
					Parameters:  &taskv1.TaskParameters{Input: []byte("payload")},
				},
			})
			require.Error(t, err)
			require.ErrorIs(t, err, task.ErrCreateRevisionMustBeOne)
		})

		t.Run("revision gaps fail", func(t *testing.T) {
			taskID := "revision-gap"
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))

			_, err := c.UpdateTask(context.Background(), &taskv1.ControlServiceUpdateTaskRequest{
				Revision: 3,
				Request: &taskv1.UpdateTaskRequest{
					TaskId:     taskID,
					Parameters: &taskv1.TaskParameters{Input: []byte("payload-new")},
				},
			})
			require.Error(t, err)
			require.ErrorIs(t, err, task.ErrRevisionGap)
		})

		t.Run("stale revisions are no-op success", func(t *testing.T) {
			taskID := "stale-revision"
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("v1"))
			mustUpdateTask(t, c, 2, taskID, []byte("v2"))

			_, err := c.UpdateTask(context.Background(), &taskv1.ControlServiceUpdateTaskRequest{
				Revision: 1,
				Request: &taskv1.UpdateTaskRequest{
					TaskId:     taskID,
					Parameters: &taskv1.TaskParameters{Input: []byte("stale")},
				},
			})
			require.NoError(t, err)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, []byte("v2"), state.Input)
		})
	})
}

func TestControllerBatchTaskOperations_BestEffort(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		resp, err := c.BatchTaskOperations(context.Background(), &taskv1.BatchTaskOperationsRequest{
			Operations: []*taskv1.BatchTaskOperation{
				{
					Operation: &taskv1.BatchTaskOperation_CreateTask{
						CreateTask: &taskv1.ControlServiceCreateTaskRequest{
							Revision: 1,
							Request: &taskv1.CreateTaskRequest{
								TaskId:      "batch-task-a",
								ExecutorId:  "executor-a",
								CallbackUrl: "https://example.com/a",
								Parameters:  &taskv1.TaskParameters{Input: []byte("v1")},
							},
						},
					},
				},
				{
					Operation: &taskv1.BatchTaskOperation_CreateTask{
						CreateTask: &taskv1.ControlServiceCreateTaskRequest{
							Revision: 2,
							Request: &taskv1.CreateTaskRequest{
								TaskId:      "batch-task-b",
								ExecutorId:  "executor-b",
								CallbackUrl: "https://example.com/b",
								Parameters:  &taskv1.TaskParameters{Input: []byte("v1")},
							},
						},
					},
				},
				{
					Operation: &taskv1.BatchTaskOperation_UpdateTask{
						UpdateTask: &taskv1.ControlServiceUpdateTaskRequest{
							Revision: 2,
							Request: &taskv1.UpdateTaskRequest{
								TaskId:     "batch-task-a",
								Parameters: &taskv1.TaskParameters{Input: []byte("v2")},
							},
						},
					},
				},
				{
					Operation: &taskv1.BatchTaskOperation_UpdateTask{
						UpdateTask: &taskv1.ControlServiceUpdateTaskRequest{
							Revision: 4,
							Request: &taskv1.UpdateTaskRequest{
								TaskId:     "batch-task-a",
								Parameters: &taskv1.TaskParameters{Input: []byte("v4")},
							},
						},
					},
				},
				{
					Operation: &taskv1.BatchTaskOperation_UpdateTask{
						UpdateTask: &taskv1.ControlServiceUpdateTaskRequest{
							Revision: 2,
							Request: &taskv1.UpdateTaskRequest{
								TaskId:     "batch-task-a",
								Parameters: &taskv1.TaskParameters{Input: []byte("duplicate")},
							},
						},
					},
				},
				{
					Operation: &taskv1.BatchTaskOperation_DeleteTask{
						DeleteTask: &taskv1.ControlServiceDeleteTaskRequest{
							Revision: 1,
							Request: &taskv1.DeleteTaskRequest{
								TaskId: "batch-missing-delete",
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetResults(), 6)

		require.Equal(t, taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED, resp.GetResults()[0].GetStatus())
		require.Equal(t, taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_FAILED_PRECONDITION, resp.GetResults()[1].GetStatus())
		require.Equal(t, taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED, resp.GetResults()[2].GetStatus())
		require.Equal(t, taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_FAILED_PRECONDITION, resp.GetResults()[3].GetStatus())
		require.Equal(t, taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_DUPLICATE, resp.GetResults()[4].GetStatus())
		require.Equal(t, taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED, resp.GetResults()[5].GetStatus())

		state, exists := readTaskState(t, db, "batch-task-a")
		require.True(t, exists)
		require.Equal(t, []byte("v2"), state.Input)
		require.Equal(t, task.LifecycleStatusSuspended, state.LifecycleStatus)
	})
}
