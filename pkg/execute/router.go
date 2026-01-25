package execute

import (
	"errors"
	"fmt"
)

type ExecutorId string

type Router interface {
	Route(executorId ExecutorId) (Executor, error)
}

type genericRouter struct {
	executors map[ExecutorId]Executor
}

type Route struct {
	Id       ExecutorId
	Executor Executor
}

func NewRouter(routes ...Route) Router {
	executors := make(map[ExecutorId]Executor, len(routes))
	for _, route := range routes {
		executors[route.Id] = route.Executor
	}
	return &genericRouter{executors: executors}
}

var (
	ErrExecutorNotFound = errors.New("executor not found")
)

func (r *genericRouter) Route(executorId ExecutorId) (Executor, error) {
	executor, ok := r.executors[executorId]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExecutorNotFound, executorId)
	}
	return executor, nil
}
