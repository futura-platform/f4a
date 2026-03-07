package f4a

import (
	"context"

	"github.com/futura-platform/f4a/internal/task"
)

// TaskIdFromContext returns the task id from the context.
// This will be set for any workflow being executed.
func TaskIdFromContext(ctx context.Context) (string, bool) {
	tk, ok := task.FromContext(ctx)
	if !ok {
		return "", false
	}
	return string(tk.Id()), true
}
