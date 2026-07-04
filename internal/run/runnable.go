package run

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/fdbexec"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/f4a/pkg/execute"
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

	// The durable homes of the two settlement flows. Namespace names are
	// durable schema: renaming one orphans the replay state of in-flight
	// tasks.
	execute.SettlementContainers
}

const (
	userFlowNamespace     = "user"
	deliveryFlowNamespace = "delivery"
)

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
		executor:   executor,
		executorId: executorId,
		db:         db.Database,
		taskKey:    taskKey,
		SettlementContainers: execute.SettlementContainers{
			User:        fdbexec.OpenTaskContainer(db, taskKey, userFlowNamespace),
			Discharge:   fdbexec.OpenTaskContainer(db, taskKey, deliveryFlowNamespace),
			DeadLetters: servicestate.NewDeadLetterParker(db, taskKey.Id()),
		},
	}
}
