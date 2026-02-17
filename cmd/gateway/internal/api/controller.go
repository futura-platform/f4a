package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/execute"
)

func NewController(
	db dbutil.DbRoot,
) (taskv1connect.ControlServiceHandler, error) {
	taskDir, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open tasks directory: %v", err)
	}
	revisionStore, err := task.CreateOrOpenRevisionStore(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open revision store: %v", err)
	}
	pendingSet, err := servicestate.CreateOrOpenReadySet(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open pending set: %v", err)
	}
	suspendedSet, err := servicestate.CreateOrOpenSuspendedSet(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open suspended set: %v", err)
	}
	return &controller{
		db:            db,
		taskDir:       taskDir,
		revisionStore: revisionStore,
		pendingSet:    pendingSet,
		suspendedSet:  suspendedSet,
	}, nil
}

type controller struct {
	db            dbutil.DbRoot
	taskDir       task.TasksDirectory
	revisionStore task.RevisionStore

	// a queue of tasks that are ready to be executed.
	pendingSet,
	// a queue of tasks that are suspended (have state, but are not executing).
	suspendedSet *reliableset.Set
}

// CreateTask implements taskv1connect.ControlServiceHandler.
func (c *controller) CreateTask(ctx context.Context, req *taskv1.ControlServiceCreateTaskRequest) (*taskv1.CreateTaskResponse, error) {
	response, _, err := c.createTaskRevisioned(ctx, req)
	return response, err
}

// UpdateTask implements taskv1connect.ControlServiceHandler.
func (c *controller) UpdateTask(ctx context.Context, req *taskv1.ControlServiceUpdateTaskRequest) (*taskv1.UpdateTaskResponse, error) {
	response, _, err := c.updateTaskRevisioned(ctx, req)
	return response, err
}

// ActivateTask implements taskv1connect.ControlServiceHandler.
func (c *controller) ActivateTask(ctx context.Context, req *taskv1.ControlServiceActivateTaskRequest) (*taskv1.ActivateTaskResponse, error) {
	response, _, err := c.activateTaskRevisioned(ctx, req)
	return response, err
}

// SuspendTask implements taskv1connect.ControlServiceHandler.
func (c *controller) SuspendTask(ctx context.Context, req *taskv1.ControlServiceSuspendTaskRequest) (*taskv1.SuspendTaskResponse, error) {
	response, _, err := c.suspendTaskRevisioned(ctx, req)
	return response, err
}

// DeleteTask implements taskv1connect.ControlServiceHandler.
func (c *controller) DeleteTask(ctx context.Context, req *taskv1.ControlServiceDeleteTaskRequest) (*taskv1.DeleteTaskResponse, error) {
	response, _, err := c.deleteTaskRevisioned(ctx, req)
	return response, err
}

// BatchTaskOperations implements taskv1connect.ControlServiceHandler.
func (c *controller) BatchTaskOperations(ctx context.Context, req *taskv1.BatchTaskOperationsRequest) (*taskv1.BatchTaskOperationsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("missing batch task operations request")
	}

	results := make([]*taskv1.BatchTaskOperationResult, 0, len(req.GetOperations()))
	for i, op := range req.GetOperations() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		result := &taskv1.BatchTaskOperationResult{
			OperationIndex: uint32(i),
		}

		switch operation := op.GetOperation().(type) {
		case *taskv1.BatchTaskOperation_CreateTask:
			response, decision, err := c.createTaskRevisioned(ctx, operation.CreateTask)
			status, message := classifyBatchResult(decision, err)
			result.Status = status
			result.ErrorMessage = message
			if err == nil {
				result.Response = &taskv1.BatchTaskOperationResult_CreateTask{CreateTask: response}
			}
		case *taskv1.BatchTaskOperation_UpdateTask:
			response, decision, err := c.updateTaskRevisioned(ctx, operation.UpdateTask)
			status, message := classifyBatchResult(decision, err)
			result.Status = status
			result.ErrorMessage = message
			if err == nil {
				result.Response = &taskv1.BatchTaskOperationResult_UpdateTask{UpdateTask: response}
			}
		case *taskv1.BatchTaskOperation_ActivateTask:
			response, decision, err := c.activateTaskRevisioned(ctx, operation.ActivateTask)
			status, message := classifyBatchResult(decision, err)
			result.Status = status
			result.ErrorMessage = message
			if err == nil {
				result.Response = &taskv1.BatchTaskOperationResult_ActivateTask{ActivateTask: response}
			}
		case *taskv1.BatchTaskOperation_SuspendTask:
			response, decision, err := c.suspendTaskRevisioned(ctx, operation.SuspendTask)
			status, message := classifyBatchResult(decision, err)
			result.Status = status
			result.ErrorMessage = message
			if err == nil {
				result.Response = &taskv1.BatchTaskOperationResult_SuspendTask{SuspendTask: response}
			}
		case *taskv1.BatchTaskOperation_DeleteTask:
			response, decision, err := c.deleteTaskRevisioned(ctx, operation.DeleteTask)
			status, message := classifyBatchResult(decision, err)
			result.Status = status
			result.ErrorMessage = message
			if err == nil {
				result.Response = &taskv1.BatchTaskOperationResult_DeleteTask{DeleteTask: response}
			}
		default:
			result.Status = taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_ERROR
			result.ErrorMessage = "missing operation payload"
		}

		results = append(results, result)
	}

	return &taskv1.BatchTaskOperationsResponse{Results: results}, nil
}

var (
	ErrNotInAnyQueue       = errors.New("task is not in any queue")
	ErrMissingInnerRequest = errors.New("missing revisioned request payload")
)

func (c *controller) createTaskRevisioned(
	_ context.Context,
	req *taskv1.ControlServiceCreateTaskRequest,
) (*taskv1.CreateTaskResponse, task.RevisionDecision, error) {
	inner := req.GetRequest()
	if inner == nil {
		return nil, 0, ErrMissingInnerRequest
	}

	decision, err := c.applyRevisionedOperation(
		task.Id(inner.GetTaskId()),
		req.GetRevision(),
		task.RevisionOperationCreate,
		func(t fdb.Transaction) error {
			tkey, err := c.taskDir.Create(t, task.Id(inner.GetTaskId()))
			if err != nil {
				if errors.Is(err, directory.ErrDirAlreadyExists) {
					return nil
				}
				return fmt.Errorf("failed to create task: %w", err)
			}
			tkey.ExecutorId().Set(t, execute.ExecutorId(inner.GetExecutorId()))
			tkey.CallbackUrl().Set(t, inner.GetCallbackUrl())
			tkey.Input().Set(t, inner.GetParameters().GetInput())
			tkey.LifecycleStatus().Set(t, task.LifecycleStatusSuspended)
			return c.suspendedSet.Add(t, []byte(tkey.Id()))
		},
	)
	if err != nil {
		return nil, decision, fmt.Errorf("failed to create task: %w", err)
	}
	return &taskv1.CreateTaskResponse{}, decision, nil
}

func (c *controller) updateTaskRevisioned(
	_ context.Context,
	req *taskv1.ControlServiceUpdateTaskRequest,
) (*taskv1.UpdateTaskResponse, task.RevisionDecision, error) {
	inner := req.GetRequest()
	if inner == nil {
		return nil, 0, ErrMissingInnerRequest
	}

	decision, err := c.applyRevisionedOperation(
		task.Id(inner.GetTaskId()),
		req.GetRevision(),
		task.RevisionOperationMutate,
		func(t fdb.Transaction) error {
			tkey, err := openTask(t, c.taskDir, inner)
			if err != nil {
				return fmt.Errorf("failed to open task: %w", err)
			}
			tkey.Input().Set(t, inner.GetParameters().GetInput())
			return nil
		},
	)
	if err != nil {
		return nil, decision, fmt.Errorf("failed to update task: %w", err)
	}
	return &taskv1.UpdateTaskResponse{}, decision, nil
}

func (c *controller) activateTaskRevisioned(
	_ context.Context,
	req *taskv1.ControlServiceActivateTaskRequest,
) (*taskv1.ActivateTaskResponse, task.RevisionDecision, error) {
	inner := req.GetRequest()
	if inner == nil {
		return nil, 0, ErrMissingInnerRequest
	}

	decision, err := c.applyRevisionedOperation(
		task.Id(inner.GetTaskId()),
		req.GetRevision(),
		task.RevisionOperationMutate,
		func(t fdb.Transaction) error {
			tkey, err := openTask(t, c.taskDir, inner)
			if err != nil {
				return fmt.Errorf("failed to open task: %w", err)
			}
			currentStatus := tkey.LifecycleStatus().Get(t).MustGet()
			switch currentStatus {
			case task.LifecycleStatusPending, task.LifecycleStatusRunning:
				return nil
			}
			tkey.LifecycleStatus().Set(t, task.LifecycleStatusPending)
			return c.pendingSet.Add(t, []byte(tkey.Id()))
		},
	)
	if err != nil {
		return nil, decision, fmt.Errorf("failed to activate task: %w", err)
	}
	return &taskv1.ActivateTaskResponse{}, decision, nil
}

func (c *controller) suspendTaskRevisioned(
	_ context.Context,
	req *taskv1.ControlServiceSuspendTaskRequest,
) (*taskv1.SuspendTaskResponse, task.RevisionDecision, error) {
	inner := req.GetRequest()
	if inner == nil {
		return nil, 0, ErrMissingInnerRequest
	}

	decision, err := c.applyRevisionedOperation(
		task.Id(inner.GetTaskId()),
		req.GetRevision(),
		task.RevisionOperationMutate,
		func(t fdb.Transaction) error {
			tkey, err := openTask(t, c.taskDir, inner)
			if err != nil {
				return fmt.Errorf("failed to open task: %w", err)
			}
			currentStatus := tkey.LifecycleStatus().Get(t).MustGet()
			if currentStatus == task.LifecycleStatusSuspended {
				return nil
			}
			if err := c.removeFromCurrentQueue(t, tkey, currentStatus); err != nil {
				return fmt.Errorf("failed to remove task from current queue: %w", err)
			}
			if err := c.suspendedSet.Add(t, []byte(tkey.Id())); err != nil {
				return fmt.Errorf("failed to add task to suspended set: %w", err)
			}
			tkey.LifecycleStatus().Set(t, task.LifecycleStatusSuspended)
			return nil
		},
	)
	if err != nil {
		return nil, decision, fmt.Errorf("failed to suspend task: %w", err)
	}
	return &taskv1.SuspendTaskResponse{}, decision, nil
}

func (c *controller) deleteTaskRevisioned(
	_ context.Context,
	req *taskv1.ControlServiceDeleteTaskRequest,
) (*taskv1.DeleteTaskResponse, task.RevisionDecision, error) {
	inner := req.GetRequest()
	if inner == nil {
		return nil, 0, ErrMissingInnerRequest
	}

	decision, err := c.applyRevisionedOperation(
		task.Id(inner.GetTaskId()),
		req.GetRevision(),
		task.RevisionOperationDelete,
		func(t fdb.Transaction) error {
			tkey, err := openTask(t, c.taskDir, inner)
			if err != nil {
				if errors.Is(err, directory.ErrDirNotExists) {
					return nil
				}
				return fmt.Errorf("failed to open task: %w", err)
			}
			currentStatus := tkey.LifecycleStatus().Get(t).MustGet()
			if err := c.removeFromCurrentQueue(t, tkey, currentStatus); err != nil &&
				!errors.Is(err, ErrNotInAnyQueue) {
				return fmt.Errorf("failed to remove task from current queue: %w", err)
			}
			return tkey.Clear(t)
		},
	)
	if err != nil {
		return nil, decision, fmt.Errorf("failed to delete task: %w", err)
	}
	return &taskv1.DeleteTaskResponse{}, decision, nil
}

func (c *controller) applyRevisionedOperation(
	id task.Id,
	revision uint64,
	operation task.RevisionOperation,
	apply func(t fdb.Transaction) error,
) (task.RevisionDecision, error) {
	result, err := c.db.Transact(func(t fdb.Transaction) (any, error) {
		decision, applyErr := c.revisionStore.Apply(
			t,
			id,
			revision,
			operation,
			func() error {
				return apply(t)
			},
		)
		if applyErr != nil {
			return nil, applyErr
		}
		return decision, nil
	})
	if err != nil {
		return 0, err
	}
	decision, ok := result.(task.RevisionDecision)
	if !ok {
		return 0, fmt.Errorf("unexpected transaction result type: %T", result)
	}
	return decision, nil
}

func classifyBatchResult(decision task.RevisionDecision, err error) (taskv1.BatchTaskOperationStatus, string) {
	if err == nil {
		if decision == task.RevisionDecisionDuplicate {
			return taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_DUPLICATE, ""
		}
		return taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_APPLIED, ""
	}

	if errors.Is(err, task.ErrInvalidRevision) ||
		errors.Is(err, task.ErrCreateRevisionMustBeOne) ||
		errors.Is(err, task.ErrRevisionGap) ||
		errors.Is(err, directory.ErrDirNotExists) ||
		errors.Is(err, ErrMissingInnerRequest) {
		return taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_FAILED_PRECONDITION, err.Error()
	}
	return taskv1.BatchTaskOperationStatus_BATCH_TASK_OPERATION_STATUS_ERROR, err.Error()
}

// removeFromCurrentQueue removes the task from the current queue.
// If the task is not in any queue, it returns ErrNotInAnyQueue.
func (c *controller) removeFromCurrentQueue(t fdb.Transaction, tkey task.TaskKey, currentStatus task.LifecycleStatus) error {
	switch currentStatus {
	case task.LifecycleStatusRunning:
		runnerId := tkey.RunnerId().Get(t).MustGet()
		taskSet, err := pool.CreateOrOpenTaskSetForRunner(c.db, runnerId)
		if err != nil {
			return fmt.Errorf("failed to create or open task set: %v", err)
		}
		if err := taskSet.Remove(t, []byte(tkey.Id())); err != nil {
			return fmt.Errorf("failed to remove task from task set: %v", err)
		}
	case task.LifecycleStatusPending:
		if err := c.pendingSet.Remove(t, []byte(tkey.Id())); err != nil {
			return fmt.Errorf("failed to remove task from ready set: %v", err)
		}
	case task.LifecycleStatusSuspended:
		return ErrNotInAnyQueue
	default:
		return fmt.Errorf("unknown status '%s'", currentStatus)
	}
	return nil
}
