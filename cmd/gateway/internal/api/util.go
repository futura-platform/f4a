package api

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/task"
)

func openTask(t fdb.Transactor, taskDir task.TasksDirectory, r interface{ GetTaskId() string }) (task.TaskKey, error) {
	return taskDir.Open(t, task.Id(r.GetTaskId()))
}
