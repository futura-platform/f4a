package execute

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/url"
	"slices"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/flog"
	"github.com/futura-platform/futura/ftype"
)

type Executable interface {
	// Settle drives the task to settlement: a terminal result is produced for
	// the input by the user-supplied flow and then discharged — delivered to
	// the callback within the bounded attempt budget, or reported as a
	// delivery failure for the dead letter queue. Scheduler-event
	// cancellations arrive via ctx and abort settlement (the task remains
	// owed); any returned error likewise means the task is NOT settled.
	Settle(
		ctx context.Context,
		marshalledInput []byte,
		callbackUrl *url.URL,
		opts ...ftype.FlowLoopOption,
	) error
}

type genericExecutable[A, R any] struct {
	genericExecutor[A, R]

	userFlow             *futura.Flow[A, R]
	callbackDeliveryFlow *futura.Flow[R, struct{}]

	marshaller ExecutionMarshaller[A, R]
	opts       []ftype.FlowLoopOption
}

// resultKey identifies an (input, result) pair for stampCompletedAt. The two
// parts are length-prefixed so distinct pairs can never collide by shifting
// bytes across the boundary, and hashing never appends into (and thereby
// mutates) the caller's input buffer.
func resultKey(marshalledInput, resultBytes []byte) [32]byte {
	h := sha256.New()
	_ = binary.Write(h, binary.LittleEndian, uint64(len(marshalledInput)))
	h.Write(marshalledInput)
	h.Write(resultBytes)
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

func (g *genericExecutable[A, R]) Settle(
	ctx context.Context,
	marshalledInput []byte,
	callbackUrl *url.URL,
	opts ...ftype.FlowLoopOption,
) error {
	output, flowErr := g.executeUserFlow(ctx, marshalledInput, opts...)
	if flowErr != nil {
		flog.FromContext(ctx).Error("failed to execute user flow", "error", flowErr)
	}
	if callbackUrl != nil {
		// no need to filter out non fatal errors types (from scheduler events), they all happen via context cancellation,
		// which will naturally skip the delivery flow
		_, err := g.callbackDeliveryFlow.Execute(ctx, func(b futura.FlowBuilder, args R) (_ struct{}, err error) {
			var resultBytes []byte
			if flowErr == nil {
				resultBytes, err = g.marshaller.MarshalOutput(output)
				if err != nil {
					return struct{}{}, err
				}
			}

			// memoize a completedAt time for the delivery backoff schedule
			completedAt, err := futura.Step(b, func(ctx context.Context, _ [32]byte) (time.Time, error) { return time.Now(), nil }, resultKey(marshalledInput, resultBytes))
			if err != nil {
				return struct{}{}, err
			}

			deliveryFailure, err := futura.Step(b, deliverResult, deliveryRequest{
				completedAt: completedAt,
				callbackUrl: *callbackUrl,
				result:      string(resultBytes),
				failure:     flowErr.Error(),
			})
			if err != nil {
				return struct{}{}, err
			} else if deliveryFailure.IsSome() {
				return struct{}{}, futura.Effect(b, func(ctx context.Context, _ struct{}) error {
					// todo: queue the task into the dead letter queue
					return nil
				}, struct{}{})
			}

			return struct{}{}, nil
		}, output)
		if err != nil {
			return fmt.Errorf("failed to discharge result: %w", err)
		}
	}

	return nil
}

func (g *genericExecutable[A, R]) executeUserFlow(ctx context.Context, marshalledInput []byte, opts ...ftype.FlowLoopOption) (R, error) {
	input, err := g.marshaller.UnmarshalInput(marshalledInput)
	if err != nil {
		return *new(R), fmt.Errorf("failed to unmarshal input: %w", err)
	}

	return g.userFlow.Execute(ctx, g.fn, input, slices.Concat(g.opts, opts)...)
}
