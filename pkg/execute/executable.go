package execute

import (
	"context"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
)

type Executable interface {
	Execute(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error)
}

type genericExecutable[A, R any] struct {
	genericExecutor[A, R]

	f *futura.Flow[A, R]

	marshaller ExecutionMarshaller[A, R]
	opts       []ftype.FlowLoopOption
}

func (g *genericExecutable[A, R]) Execute(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) ([]byte, error) {
	input, err := g.marshaller.UnmarshalInput(marshalledInput)
	if err != nil {
		return nil, err
	}

	output, err := g.f.Execute(ctx, g.fn, input, append(g.opts, opts...)...)
	if err != nil {
		return nil, err
	}

	return g.marshaller.MarshalOutput(output)
}
