package run

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/fdbexec"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/execute"
	"github.com/futura-platform/futura/ftype/executiontype"
)

// Runnable represents a locally executable task, bound to an input.
// It will trigger an instant replay if the input changes.
// Runnable execution is a distributed singleton,
// meaning that only one instance of the execution will run for a given input.
type Runnable struct {
	executor   execute.Executor
	executorId execute.ExecutorId

	db      fdb.Database
	taskKey task.TaskKey

	userContainer, callbackDeliveryContainer executiontype.TransactionalContainer
}

func (r Runnable) Id() task.Id {
	return r.taskKey.Id()
}

func (r Runnable) ExecutorId() execute.ExecutorId {
	return r.executorId
}

func (r Runnable) TaskKey() task.TaskKey {
	return r.taskKey
}

func (r Runnable) Db() fdb.Database {
	return r.db
}

func NewRunnable(
	executor execute.Executor,
	executorId execute.ExecutorId,
	db dbutil.DbRoot,
	taskKey task.TaskKey,
) Runnable {
	return Runnable{
		executor:                  executor,
		executorId:                executorId,
		db:                        db.Database,
		taskKey:                   taskKey,
		userContainer:             fdbexec.OpenTaskContainer(db, taskKey, "user"),
		callbackDeliveryContainer: fdbexec.OpenTaskContainer(db, taskKey, "callback_delivery"),
	}
}
