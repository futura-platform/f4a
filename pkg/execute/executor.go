package execute

import (
	"context"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
)

type Executor interface {
	ExecuteFrom(executiontype.TransactionalContainer) Executable
}

type executionResult[R any] struct {
	result     R
	finishedAt time.Time
}

type genericExecutor[A, R any] struct {
	fn         futura.FlowFn[A, executionResult[R]]
	marshaller ExecutionMarshaller[A, R]
	opts       []ftype.FlowLoopOption
}

func NewExecutor[A, R any](
	fn futura.FlowFn[A, R],
	marshaller ExecutionMarshaller[A, R],
	opts ...ftype.FlowLoopOption,
) Executor {
	return &genericExecutor[A, R]{fn: func(b futura.FlowBuilder, args A) (executionResult[R], error) {
		result, err := fn(b, args)
		if err != nil {
			return executionResult[R]{}, err
		}
		// bind the result to the current time
		finishedAt, err := futura.Source(b, func(ctx context.Context) (time.Time, error) {
			return time.Now(), nil
		})
		if err != nil {
			return executionResult[R]{}, err
		}
		return executionResult[R]{result: result, finishedAt: finishedAt}, nil
	}, marshaller: marshaller, opts: opts}
}

// ExecuteFrom implements Executor.
func (e genericExecutor[A, R]) ExecuteFrom(c executiontype.TransactionalContainer) Executable {
	f := futura.NewFlowFromContainer[A, executionResult[R]](c)
	return &genericExecutable[A, R]{
		genericExecutor: e,
		f:               f,
		marshaller:      e.marshaller,
		opts:            e.opts,
	}
}
