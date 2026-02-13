package run

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype/executiontype"
)

// Runnable represents a locally executable task, bound to an input.
// It will trigger an instant replay if the input changes.
// Runnable execution is a distributed singleton,
// meaning that only one instance of the execution will run for a given input.
type Runnable struct {
	executor execute.Executor

	db      fdb.Database
	taskKey task.TaskKey

	execution executiontype.TransactionalContainer
}

func (r Runnable) Id() task.Id {
	return r.taskKey.Id()
}

func (r Runnable) TaskKey() task.TaskKey {
	return r.taskKey
}

func NewRunnable(
	executor execute.Executor,
	db fdb.Database,
	taskKey task.TaskKey,
	execution executiontype.TransactionalContainer,
) Runnable {
	return Runnable{
		executor:  executor,
		db:        db,
		taskKey:   taskKey,
		execution: execution,
	}
}
