package api

import (
	"github.com/futura-platform/f4a/internal/task"
)

func (c *controller) openTask(r interface{ GetTaskId() string }) (task.TaskKey, error) {
	return c.taskDir.Open(c.db, task.Id(r.GetTaskId()))
}
