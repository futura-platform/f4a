package api

import (
	"context"
	"errors"
	"math"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newTestController(t *testing.T, db dbutil.DbRoot) *controller {
	t.Helper()

	handler, releaseController, err := NewController(db)
	require.NoError(t, err)
	t.Cleanup(releaseController)

	c, ok := handler.(*controller)
	require.True(t, ok)
	return c
}

func testResourceRequest() *taskv1.TaskResourceRequest {
	return taskv1.TaskResourceRequest_builder{
		CpuMillis:   proto.Uint32(500),
		MemoryBytes: proto.Uint64(1024),
	}.Build()
}

type taskState struct {
	ExecutorID      string
	CallbackURL     *string
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
	_, err := c.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
		Revision: proto.Uint64(revision),
		Request: taskv1.CreateTaskRequest_builder{
			TaskId:          proto.String(taskID),
			ExecutorId:      proto.String(executorID),
			CallbackUrl:     proto.String(callbackURL),
			Parameters:      taskv1.TaskParameters_builder{Input: input}.Build(),
			ResourceRequest: testResourceRequest(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
}

func mustUpdateTask(t *testing.T, c *controller, revision uint64, taskID string, input []byte) {
	t.Helper()
	_, err := c.UpdateTask(context.Background(), taskv1.ControlServiceUpdateTaskRequest_builder{
		Revision: proto.Uint64(revision),
		Request: taskv1.UpdateTaskRequest_builder{
			TaskId:     proto.String(taskID),
			Parameters: taskv1.TaskParameters_builder{Input: input}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
}

func mustActivateTask(t *testing.T, c *controller, revision uint64, taskID string) {
	t.Helper()
	_, err := c.ActivateTask(context.Background(), taskv1.ControlServiceActivateTaskRequest_builder{
		Revision: proto.Uint64(revision),
		Request: taskv1.ActivateTaskRequest_builder{
			TaskId: proto.String(taskID),
		}.Build(),
	}.Build())
	require.NoError(t, err)
}

func mustSuspendTask(t *testing.T, c *controller, revision uint64, taskID string) {
	t.Helper()
	_, err := c.SuspendTask(context.Background(), taskv1.ControlServiceSuspendTaskRequest_builder{
		Revision: proto.Uint64(revision),
		Request: taskv1.SuspendTaskRequest_builder{
			TaskId: proto.String(taskID),
		}.Build(),
	}.Build())
	require.NoError(t, err)
}

func mustDeleteTask(t *testing.T, c *controller, revision uint64, taskID string) {
	t.Helper()
	_, err := c.DeleteTask(context.Background(), taskv1.ControlServiceDeleteTaskRequest_builder{
		Revision: proto.Uint64(revision),
		Request: taskv1.DeleteTaskRequest_builder{
			TaskId: proto.String(taskID),
		}.Build(),
	}.Build())
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

func setTaskRunnerAndLifecycleStatus(
	t *testing.T,
	db dbutil.DbRoot,
	taskID string,
	runnerID string,
	status task.LifecycleStatus,
) {
	t.Helper()

	taskDir, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)

	_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
		tkey, err := taskDir.Open(tx, task.Id(taskID))
		if err != nil {
			return nil, err
		}
		tkey.RunnerId().Set(tx, &runnerID)
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
			callbackURL := "https://example.com/a"
			expected := taskState{
				ExecutorID:      "executor-a",
				CallbackURL:     &callbackURL,
				Input:           []byte("payload-a"),
				LifecycleStatus: task.LifecycleStatusSuspended,
			}
			mustCreateTask(t, c, 1, taskID, expected.ExecutorID, callbackURL, expected.Input)

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

		t.Run("without callback url", func(t *testing.T) {
			taskID := "create-no-callback"
			input := []byte("payload-no-callback")
			_, err := c.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
				Revision: proto.Uint64(1),
				Request: taskv1.CreateTaskRequest_builder{
					TaskId:          proto.String(taskID),
					ExecutorId:      proto.String("executor-a"),
					Parameters:      taskv1.TaskParameters_builder{Input: input}.Build(),
					ResourceRequest: testResourceRequest(),
				}.Build(),
			}.Build())
			require.NoError(t, err)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, taskState{
				ExecutorID:      "executor-a",
				CallbackURL:     nil,
				Input:           input,
				LifecycleStatus: task.LifecycleStatusSuspended,
			}, state)
		})

		t.Run("rejects memory request above int64 max", func(t *testing.T) {
			_, controlHandler := taskv1connect.NewControlServiceHandler(
				c,
				connect.WithInterceptors(validate.NewInterceptor()),
			)
			server := testutil.NewEphemeralHTTPServer(t, controlHandler.ServeHTTP)
			client := taskv1connect.NewControlServiceClient(server.Client(), server.URL)

			taskID := "create-memory-overflow"
			_, err := client.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
				Revision: proto.Uint64(1),
				Request: taskv1.CreateTaskRequest_builder{
					TaskId:      proto.String(taskID),
					ExecutorId:  proto.String("executor-a"),
					CallbackUrl: proto.String("https://example.com/a"),
					Parameters:  taskv1.TaskParameters_builder{Input: []byte("payload-a")}.Build(),
					ResourceRequest: taskv1.TaskResourceRequest_builder{
						CpuMillis:   proto.Uint32(500),
						MemoryBytes: proto.Uint64(uint64(math.MaxInt64) + 1),
					}.Build(),
				}.Build(),
			}.Build())
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Contains(t, err.Error(), "memory_bytes")

			_, exists := readTaskState(t, db, taskID)
			require.False(t, exists)
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
			require.Equal(t, "https://example.com/a", *expected.CallbackURL)
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
		setTaskRunnerAndLifecycleStatus(t, db, taskID, "runner-a", task.LifecycleStatusRunning)
		mustActivateTask(t, c, 2, taskID)

		state, exists := readTaskState(t, db, taskID)
		require.True(t, exists)
		require.Equal(t, task.LifecycleStatusRunning, state.LifecycleStatus)
	})
}

func TestControllerSuspendTask_RunningTaskMissingQueueIsInvariantViolation(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)
		taskID := "suspend-running-missing-queue"

		mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))
		setTaskRunnerAndLifecycleStatus(t, db, taskID, "missing-runner", task.LifecycleStatusRunning)

		_, err := c.SuspendTask(context.Background(), taskv1.ControlServiceSuspendTaskRequest_builder{
			Revision: proto.Uint64(2),
			Request: taskv1.SuspendTaskRequest_builder{
				TaskId: proto.String(taskID),
			}.Build(),
		}.Build())
		require.ErrorIs(t, err, servicestate.ErrRunnerSetDoesNotExist)
	})
}

func TestControllerRevisionRules(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("create revision must be one", func(t *testing.T) {
			_, err := c.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
				Revision: proto.Uint64(2),
				Request: taskv1.CreateTaskRequest_builder{
					TaskId:          proto.String("bad-create-revision"),
					ExecutorId:      proto.String("executor-a"),
					CallbackUrl:     proto.String("https://example.com/a"),
					Parameters:      taskv1.TaskParameters_builder{Input: []byte("payload")}.Build(),
					ResourceRequest: testResourceRequest(),
				}.Build(),
			}.Build())
			require.Error(t, err)
			require.ErrorIs(t, err, task.ErrCreateRevisionMustBeOne)
		})

		t.Run("revision gaps fail", func(t *testing.T) {
			taskID := "revision-gap"
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))

			_, err := c.UpdateTask(context.Background(), taskv1.ControlServiceUpdateTaskRequest_builder{
				Revision: proto.Uint64(3),
				Request: taskv1.UpdateTaskRequest_builder{
					TaskId:     proto.String(taskID),
					Parameters: taskv1.TaskParameters_builder{Input: []byte("payload-new")}.Build(),
				}.Build(),
			}.Build())
			require.Error(t, err)
			require.ErrorIs(t, err, task.ErrRevisionGap)
		})

		t.Run("stale revisions are no-op success", func(t *testing.T) {
			taskID := "stale-revision"
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("v1"))
			mustUpdateTask(t, c, 2, taskID, []byte("v2"))

			_, err := c.UpdateTask(context.Background(), taskv1.ControlServiceUpdateTaskRequest_builder{
				Revision: proto.Uint64(1),
				Request: taskv1.UpdateTaskRequest_builder{
					TaskId:     proto.String(taskID),
					Parameters: taskv1.TaskParameters_builder{Input: []byte("stale")}.Build(),
				}.Build(),
			}.Build())
			require.NoError(t, err)

			state, exists := readTaskState(t, db, taskID)
			require.True(t, exists)
			require.Equal(t, []byte("v2"), state.Input)
		})
	})
}

func TestControllerRejectsMissingParameters(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		t.Run("create requires parameters", func(t *testing.T) {
			_, err := c.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
				Revision: proto.Uint64(1),
				Request: taskv1.CreateTaskRequest_builder{
					TaskId:          proto.String("missing-create-params"),
					ExecutorId:      proto.String("executor-a"),
					CallbackUrl:     proto.String("https://example.com/a"),
					Parameters:      nil,
					ResourceRequest: testResourceRequest(),
				}.Build(),
			}.Build())
			require.ErrorIs(t, err, ErrMissingParameters)
		})

		t.Run("update requires parameters", func(t *testing.T) {
			taskID := "missing-update-params"
			mustCreateTask(t, c, 1, taskID, "executor-a", "https://example.com/a", []byte("payload"))
			_, err := c.UpdateTask(context.Background(), taskv1.ControlServiceUpdateTaskRequest_builder{
				Revision: proto.Uint64(2),
				Request: taskv1.UpdateTaskRequest_builder{
					TaskId:     proto.String(taskID),
					Parameters: nil,
				}.Build(),
			}.Build())
			require.ErrorIs(t, err, ErrMissingParameters)
		})
	})
}

func TestControllerRejectsMissingResourceRequest(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		_, err := c.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
			Revision: proto.Uint64(1),
			Request: taskv1.CreateTaskRequest_builder{
				TaskId:          proto.String("missing-resource-request"),
				ExecutorId:      proto.String("executor-a"),
				CallbackUrl:     proto.String("https://example.com/a"),
				Parameters:      taskv1.TaskParameters_builder{Input: []byte("payload")}.Build(),
				ResourceRequest: nil,
			}.Build(),
		}.Build())
		require.ErrorIs(t, err, ErrMissingResourceRequest)
	})
}

func TestCreateTaskProtovalidateRejectsMissingResourceRequest(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)
		_, controlHandler := taskv1connect.NewControlServiceHandler(
			c,
			connect.WithInterceptors(validate.NewInterceptor()),
		)
		server := testutil.NewEphemeralHTTPServer(t, controlHandler.ServeHTTP)
		client := taskv1connect.NewControlServiceClient(server.Client(), server.URL)

		taskID := "protovalidate-missing-resource-request"
		_, err := client.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
			Revision: proto.Uint64(1),
			Request: taskv1.CreateTaskRequest_builder{
				TaskId:      proto.String(taskID),
				ExecutorId:  proto.String("executor-a"),
				CallbackUrl: proto.String("https://example.com/a"),
				Parameters:  taskv1.TaskParameters_builder{Input: []byte("payload")}.Build(),
			}.Build(),
		}.Build())
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.Contains(t, err.Error(), "resource_request")

		_, exists := readTaskState(t, db, taskID)
		require.False(t, exists)
	})
}

func TestCreateTaskProtovalidateRejectsZeroResourceRequest(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)
		_, controlHandler := taskv1connect.NewControlServiceHandler(
			c,
			connect.WithInterceptors(validate.NewInterceptor()),
		)
		server := testutil.NewEphemeralHTTPServer(t, controlHandler.ServeHTTP)
		client := taskv1connect.NewControlServiceClient(server.Client(), server.URL)

		taskID := "protovalidate-zero-resource-request"
		_, err := client.CreateTask(context.Background(), taskv1.ControlServiceCreateTaskRequest_builder{
			Revision: proto.Uint64(1),
			Request: taskv1.CreateTaskRequest_builder{
				TaskId:      proto.String(taskID),
				ExecutorId:  proto.String("executor-a"),
				CallbackUrl: proto.String("https://example.com/a"),
				Parameters:  taskv1.TaskParameters_builder{Input: []byte("payload")}.Build(),
				ResourceRequest: taskv1.TaskResourceRequest_builder{
					CpuMillis:   proto.Uint32(0),
					MemoryBytes: proto.Uint64(0),
				}.Build(),
			}.Build(),
		}.Build())
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		require.Contains(t, err.Error(), "cpu_millis")
		require.Contains(t, err.Error(), "memory_bytes")

		_, exists := readTaskState(t, db, taskID)
		require.False(t, exists)
	})
}

func TestControllerBatchTaskOperations_BestEffort(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		c := newTestController(t, db)

		operations := []*taskv1.BatchTaskOperation{
			taskv1.BatchTaskOperation_builder{
				CreateTask: taskv1.ControlServiceCreateTaskRequest_builder{
					Revision: proto.Uint64(1),
					Request: taskv1.CreateTaskRequest_builder{
						TaskId:          proto.String("batch-task-a"),
						ExecutorId:      proto.String("executor-a"),
						CallbackUrl:     proto.String("https://example.com/a"),
						Parameters:      taskv1.TaskParameters_builder{Input: []byte("v1")}.Build(),
						ResourceRequest: testResourceRequest(),
					}.Build(),
				}.Build(),
			}.Build(),
			taskv1.BatchTaskOperation_builder{
				CreateTask: taskv1.ControlServiceCreateTaskRequest_builder{
					Revision: proto.Uint64(2),
					Request: taskv1.CreateTaskRequest_builder{
						TaskId:          proto.String("batch-task-b"),
						ExecutorId:      proto.String("executor-b"),
						CallbackUrl:     proto.String("https://example.com/b"),
						Parameters:      taskv1.TaskParameters_builder{Input: []byte("v1")}.Build(),
						ResourceRequest: testResourceRequest(),
					}.Build(),
				}.Build(),
			}.Build(),
			taskv1.BatchTaskOperation_builder{
				UpdateTask: taskv1.ControlServiceUpdateTaskRequest_builder{
					Revision: proto.Uint64(2),
					Request: taskv1.UpdateTaskRequest_builder{
						TaskId:     proto.String("batch-task-a"),
						Parameters: taskv1.TaskParameters_builder{Input: []byte("v2")}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			taskv1.BatchTaskOperation_builder{
				UpdateTask: taskv1.ControlServiceUpdateTaskRequest_builder{
					Revision: proto.Uint64(4),
					Request: taskv1.UpdateTaskRequest_builder{
						TaskId:     proto.String("batch-task-a"),
						Parameters: taskv1.TaskParameters_builder{Input: []byte("v4")}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			taskv1.BatchTaskOperation_builder{
				UpdateTask: taskv1.ControlServiceUpdateTaskRequest_builder{
					Revision: proto.Uint64(2),
					Request: taskv1.UpdateTaskRequest_builder{
						TaskId:     proto.String("batch-task-a"),
						Parameters: taskv1.TaskParameters_builder{Input: []byte("duplicate")}.Build(),
					}.Build(),
				}.Build(),
			}.Build(),
			taskv1.BatchTaskOperation_builder{
				DeleteTask: taskv1.ControlServiceDeleteTaskRequest_builder{
					Revision: proto.Uint64(1),
					Request: taskv1.DeleteTaskRequest_builder{
						TaskId: proto.String("batch-missing-delete"),
					}.Build(),
				}.Build(),
			}.Build(),
		}
		resp, err := c.BatchTaskOperations(context.Background(), taskv1.BatchTaskOperationsRequest_builder{
			Operations: operations,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetResults(), 6)

		expectedStatuses := []taskv1.BatchTaskOperationStatus{
			taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED,
			taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_FAILED_PRECONDITION,
			taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED,
			taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_FAILED_PRECONDITION,
			taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_DUPLICATE,
			taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED,
		}
		for i, operation := range operations {
			result := resp.GetResults()[i]
			require.Equal(t, expectedStatuses[i], result.GetStatus())

			switch operation.WhichOperation() {
			case taskv1.BatchTaskOperation_CreateTask_case:
				if result.GetStatus() == taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED {
					require.Equal(t, taskv1.BatchTaskOperationResult_CreateTask_case, result.WhichResponse())
					require.NotNil(t, result.GetCreateTask())
				} else {
					require.Equal(t, taskv1.BatchTaskOperationResult_Response_not_set_case, result.WhichResponse())
				}
			case taskv1.BatchTaskOperation_UpdateTask_case:
				switch result.GetStatus() {
				case taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED,
					taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_DUPLICATE:
					require.Equal(t, taskv1.BatchTaskOperationResult_UpdateTask_case, result.WhichResponse())
				default:
					require.Equal(t, taskv1.BatchTaskOperationResult_Response_not_set_case, result.WhichResponse())
				}
				if result.GetStatus() == taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED {
					require.NotNil(t, result.GetUpdateTask())
				}
			case taskv1.BatchTaskOperation_DeleteTask_case:
				if result.GetStatus() == taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED {
					require.Equal(t, taskv1.BatchTaskOperationResult_DeleteTask_case, result.WhichResponse())
					require.NotNil(t, result.GetDeleteTask())
				} else {
					require.Equal(t, taskv1.BatchTaskOperationResult_Response_not_set_case, result.WhichResponse())
				}
			default:
				t.Fatalf("unexpected batch test operation type at index %d: %v", i, operation.WhichOperation())
			}
		}

		state, exists := readTaskState(t, db, "batch-task-a")
		require.True(t, exists)
		require.Equal(t, []byte("v2"), state.Input)
		require.Equal(t, task.LifecycleStatusSuspended, state.LifecycleStatus)
	})
}
