package execute

import (
	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/executiontype"
)

type Executor interface {
	ExecuteFrom(executiontype.TransactionalContainer) Executable
}

type genericExecutor[A, R any] struct {
	fn         futura.FlowFn[A, R]
	marshaller ExecutionMarshaller[A, R]
}

func NewExecutor[A, R any](fn futura.FlowFn[A, R], marshaller ExecutionMarshaller[A, R]) Executor {
	return &genericExecutor[A, R]{fn: fn, marshaller: marshaller}
}

// ExecuteFrom implements Executor.
func (e genericExecutor[A, R]) ExecuteFrom(c executiontype.TransactionalContainer) Executable {
	f := futura.NewFlowFromContainer[A, R](c)
	return &genericExecutable[A, R]{
		genericExecutor: e,
		f:               f,
		marshaller:      e.marshaller,
	}
}
