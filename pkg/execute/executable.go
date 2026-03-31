package execute

import (
	"context"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
)

type Executable interface {
	Execute(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) (
		result []byte,
		finishedAt time.Time,
		err error,
	)
}

type genericExecutable[A, R any] struct {
	genericExecutor[A, R]

	f *futura.Flow[A, executionResult[R]]

	marshaller ExecutionMarshaller[A, R]
	opts       []ftype.FlowLoopOption
}

func (g *genericExecutable[A, R]) Execute(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) (
	result []byte, finishedAt time.Time, err error,
) {
	input, err := g.marshaller.UnmarshalInput(marshalledInput)
	if err != nil {
		return nil, time.Now(), err
	}

	output, err := g.f.Execute(ctx, g.fn, input, append(g.opts, opts...)...)
	if err != nil {
		return nil, time.Now(), err
	}

	result, err = g.marshaller.MarshalOutput(output.result)
	return result, output.finishedAt, err
}
