package execute

import (
	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
)

type Executor interface {
	ExecuteFrom(executiontype.TransactionalContainer) Executable
}

type genericExecutor[A, R any] struct {
	fn         futura.FlowFn[A, R]
	marshaller ExecutionMarshaller[A, R]
	opts       []ftype.FlowLoopOption
}

func NewExecutor[A, R any](fn futura.FlowFn[A, R], marshaller ExecutionMarshaller[A, R], opts ...ftype.FlowLoopOption) Executor {
	return &genericExecutor[A, R]{fn: fn, marshaller: marshaller, opts: opts}
}

// ExecuteFrom implements Executor.
func (e genericExecutor[A, R]) ExecuteFrom(c executiontype.TransactionalContainer) Executable {
	f := futura.NewFlowFromContainer[A, R](c)
	return &genericExecutable[A, R]{
		genericExecutor: e,
		f:               f,
		marshaller:      e.marshaller,
		opts:            e.opts,
	}
}
