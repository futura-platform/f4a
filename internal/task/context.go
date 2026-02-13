package task

import "context"

type contextKey string

const (
	taskKeyContextKey contextKey = "taskKey"
)

func WithTaskKey(ctx context.Context, taskKey TaskKey) context.Context {
	return context.WithValue(ctx, taskKeyContextKey, taskKey)
}

func FromContext(ctx context.Context) (TaskKey, bool) {
	taskKey, ok := ctx.Value(taskKeyContextKey).(TaskKey)
	return taskKey, ok
}
