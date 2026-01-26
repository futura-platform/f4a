package api

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	"github.com/futura-platform/f4a/internal/pool"
	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/f4a/pkg/util"
)

func NewController(
	db util.DbRoot,
	readySet, suspendedSet *reliableset.Set,
) (taskv1connect.ControlServiceHandler, error) {
	taskDir, err := task.CreateOrOpenTasksDirectory(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create or open tasks directory: %v", err)
	}
	return &controller{
		db:           db,
		taskDir:      taskDir,
		readySet:     readySet,
		suspendedSet: suspendedSet,
	}, nil
}

type controller struct {
	db      util.DbRoot
	taskDir task.TasksDirectory

	// a queue of tasks that are ready to be executed.
	readySet,
	// a queue of tasks that are suspended (have state, but are not executing).
	suspendedSet *reliableset.Set
}

// CreateTask implements taskv1connect.ControlServiceHandler.
func (c *controller) CreateTask(ctx context.Context, req *taskv1.CreateTaskRequest) (*taskv1.CreateTaskResponse, error) {
	id := task.NewId()
	tkey, err := c.taskDir.Create(c.db, id)
	if err != nil {
		return nil, fmt.Errorf("failed to create task key: %v", err)
	}
	_, err = c.db.Transact(func(t fdb.Transaction) (any, error) {
		tkey.ExecutorId().Set(t, execute.ExecutorId(req.ExecutorId))
		tkey.CallbackUrl().Set(t, req.CallbackUrl)
		tkey.Input().Set(t, req.Parameters.Input)
		tkey.LifecycleStatus().Set(t, task.LifecycleStatusSuspended)
		return nil, c.suspendedSet.Add(t, id.Bytes())
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %v", err)
	}
	return &taskv1.CreateTaskResponse{TaskId: id.String()}, nil
}

// UpdateTask implements taskv1connect.ControlServiceHandler.
func (c *controller) UpdateTask(ctx context.Context, req *taskv1.UpdateTaskRequest) (*taskv1.UpdateTaskResponse, error) {
	id, err := task.IdFromString(req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %v", err)
	}
	tkey, err := c.taskDir.Open(c.db, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get task key: %v", err)
	}
	_, err = c.db.Transact(func(t fdb.Transaction) (any, error) {
		tkey.Input().Set(t, req.Parameters.Input)
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update task: %v", err)
	}
	return &taskv1.UpdateTaskResponse{}, nil
}

// ActivateTask implements taskv1connect.ControlServiceHandler.
func (c *controller) ActivateTask(ctx context.Context, req *taskv1.ActivateTaskRequest) (*taskv1.ActivateTaskResponse, error) {
	id, err := task.IdFromString(req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %v", err)
	}

	tkey, err := c.taskDir.Open(c.db, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get task key: %v", err)
	}
	_, err = c.db.Transact(func(t fdb.Transaction) (any, error) {
		currentStatus := tkey.LifecycleStatus().Get(t).MustGet()
		if currentStatus != task.LifecycleStatusSuspended {
			return nil, fmt.Errorf("expected task to be suspended, got %s", currentStatus)
		}
		tkey.LifecycleStatus().Set(t, task.LifecycleStatusPending)
		return nil, c.readySet.Add(t, id.Bytes())
	})
	if err != nil {
		return nil, fmt.Errorf("failed to activate task: %v", err)
	}
	return &taskv1.ActivateTaskResponse{}, nil
}

// DeleteTask implements taskv1connect.ControlServiceHandler.
func (c *controller) DeleteTask(ctx context.Context, req *taskv1.DeleteTaskRequest) (*taskv1.DeleteTaskResponse, error) {
	id, err := task.IdFromString(req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %v", err)
	}
	tkey, err := c.taskDir.Open(c.db, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get task key: %v", err)
	}
	_, err = c.db.Transact(func(t fdb.Transaction) (any, error) {
		currentStatus := tkey.LifecycleStatus().Get(t).MustGet()
		if err := c.removeFromCurrentQueue(t, tkey, currentStatus); err != nil {
			return nil, fmt.Errorf("failed to remove task from current queue: %v", err)
		}

		return nil, tkey.Clear(t)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to delete task: %v", err)
	}
	return &taskv1.DeleteTaskResponse{}, nil
}

// SuspendTask implements taskv1connect.ControlServiceHandler.
func (c *controller) SuspendTask(ctx context.Context, req *taskv1.SuspendTaskRequest) (*taskv1.SuspendTaskResponse, error) {
	id, err := task.IdFromString(req.TaskId)
	if err != nil {
		return nil, fmt.Errorf("failed to parse task id: %v", err)
	}
	tkey, err := c.taskDir.Open(c.db, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get task key: %v", err)
	}
	_, err = c.db.Transact(func(t fdb.Transaction) (any, error) {
		currentStatus := tkey.LifecycleStatus().Get(t).MustGet()
		if currentStatus == task.LifecycleStatusSuspended {
			return nil, fmt.Errorf("task is already suspended")
		}
		if err := c.removeFromCurrentQueue(t, tkey, currentStatus); err != nil {
			return nil, fmt.Errorf("failed to remove task from current queue: %v", err)
		}

		if err := c.suspendedSet.Add(t, tkey.Id().Bytes()); err != nil {
			return nil, fmt.Errorf("failed to add task to suspended set: %v", err)
		}
		tkey.LifecycleStatus().Set(t, task.LifecycleStatusSuspended)
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to suspend task: %v", err)
	}
	return &taskv1.SuspendTaskResponse{}, nil
}

func (c *controller) removeFromCurrentQueue(t fdb.Transaction, tkey task.TaskKey, currentStatus task.LifecycleStatus) error {
	switch currentStatus {
	case task.LifecycleStatusRunning:
		runnerId := tkey.RunnerId().Get(t).MustGet()
		taskSet, err := pool.CreateOrOpenTaskSetForRunner(c.db, runnerId)
		if err != nil {
			return fmt.Errorf("failed to create or open task set: %v", err)
		}
		if err := taskSet.Remove(t, tkey.Id().Bytes()); err != nil {
			return fmt.Errorf("failed to remove task from task set: %v", err)
		}
	case task.LifecycleStatusPending:
		if err := c.readySet.Remove(t, tkey.Id().Bytes()); err != nil {
			return fmt.Errorf("failed to remove task from ready set: %v", err)
		}
	default:
		return fmt.Errorf("expected task to be running or pending, got %s", currentStatus)
	}
	return nil
}
