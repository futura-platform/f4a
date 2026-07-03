package execute

import (
	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
)

type SettlementContainers struct {
	User      executiontype.TransactionalContainer
	Discharge executiontype.TransactionalContainer
}

type Executor interface {
	ExecuteFrom(SettlementContainers) Executable
}

type genericExecutor[A, R any] struct {
	fn         futura.FlowFn[A, R]
	marshaller ExecutionMarshaller[A, R]
	opts       []ftype.FlowLoopOption
}

func NewExecutor[A, R any](
	fn futura.FlowFn[A, R],
	marshaller ExecutionMarshaller[A, R],
	opts ...ftype.FlowLoopOption,
) Executor {
	return &genericExecutor[A, R]{fn: fn, marshaller: marshaller, opts: opts}
}

// ExecuteFrom implements Executor.
func (e genericExecutor[A, R]) ExecuteFrom(s SettlementContainers) Executable {
	userFlow := futura.NewFlowFromContainer[A, R](s.User)
	callbackDeliveryFlow := futura.NewFlowFromContainer[R, struct{}](s.Discharge)
	return &genericExecutable[A, R]{
		genericExecutor:      e,
		userFlow:             userFlow,
		callbackDeliveryFlow: callbackDeliveryFlow,
		marshaller:           e.marshaller,
		opts:                 e.opts,
	}
}
