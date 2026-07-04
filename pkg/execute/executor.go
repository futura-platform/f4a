package execute

import (
	"context"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
)

// DeadLetterParker durably records that a task's terminal result could not
// be delivered within the callback attempt budget, so the task can still
// settle. Implementations are expected to be bound to a specific task.
type DeadLetterParker interface {
	Park(ctx context.Context, result *taskv1.TaskResult) error
}

type SettlementContainers struct {
	User      executiontype.TransactionalContainer
	Discharge executiontype.TransactionalContainer
	// DeadLetters absorbs results whose delivery budget is exhausted.
	// It may be nil only when no callback delivery can occur.
	DeadLetters DeadLetterParker
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
		deadLetters:          s.DeadLetters,
		marshaller:           e.marshaller,
		opts:                 e.opts,
	}
}
