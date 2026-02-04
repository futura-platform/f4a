package execute

type ExecutorId string

type Router interface {
	Route(executorId ExecutorId) Executor
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

func (r *genericRouter) Route(executorId ExecutorId) Executor {
	executor, ok := r.executors[executorId]
	if !ok {
		return notFoundExecutor{requestedExecutorId: executorId}
	}
	return executor
}
