package execute

import (
	"context"
	"crypto/sha256"
	"net/url"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype"
	"github.com/samber/mo"
)

type Executable interface {
	// Execute executes the executable.
	Execute(
		ctx context.Context,
		marshalledInput []byte,
		callbackUrl *url.URL,
		opts ...ftype.FlowLoopOption,
	) (callbackDeliveryFailure mo.Option[string], err error)
}

type genericExecutable[A, R any] struct {
	genericExecutor[A, R]

	f            *futura.Flow[A, R]
	callbackFlow *futura.Flow[R, mo.Option[string]]

	marshaller ExecutionMarshaller[A, R]
	opts       []ftype.FlowLoopOption
}

func (g *genericExecutable[A, R]) Execute(ctx context.Context, marshalledInput []byte, callbackUrl *url.URL, opts ...ftype.FlowLoopOption) (callbackDeliveryFailure mo.Option[string], err error) {
	input, err := g.marshaller.UnmarshalInput(marshalledInput)
	if err != nil {
		return mo.None[string](), err
	}

	output, flowErr := g.f.Execute(ctx, g.fn, input, append(g.opts, opts...)...)
	if callbackUrl != nil {
		// no need to filter out non fatal errors types (from scheduler events), they all happen via context cancellation,
		// which will naturally skip the callback delivery step
		callbackDeliveryFailure, err = g.callbackFlow.Execute(ctx, func(b futura.FlowBuilder, args R) (mo.Option[string], error) {
			var resultBytes []byte
			if flowErr == nil {
				resultBytes, err = g.marshaller.MarshalOutput(output)
				if err != nil {
					return mo.None[string](), err
				}
			}

			completedAt, err := futura.Step(
				b,
				func(ctx context.Context, resultHash [32]byte) (time.Time, error) { return time.Now(), nil },
				sha256.Sum256(append(marshalledInput, resultBytes...)),
			)
			if err != nil {
				return mo.None[string](), err
			}

			return futura.Step(b, deliverCallback, taskResult{
				completedAt: completedAt,
				callbackUrl: *callbackUrl,
				result:      string(resultBytes),
				failure:     flowErr.Error(),
			})
		}, output)
	}

	return callbackDeliveryFailure, nil
}
