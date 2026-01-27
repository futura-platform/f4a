package api

import (
	"fmt"

	"github.com/futura-platform/f4a/internal/task"
)

type taskAddressed interface {
	GetTaskId() string
}

func (c *controller) openTask(r taskAddressed) (task.TaskKey, error) {
	id, err := task.IdFromString(r.GetTaskId())
	if err != nil {
		return task.TaskKey{}, fmt.Errorf("failed to parse task id: %w", err)
	}
	return c.taskDir.Open(c.db, id)
}
