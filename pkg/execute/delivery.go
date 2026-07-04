package execute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/seal"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"schneider.vip/problem"
)

type deliveryRequest struct {
	completedAt time.Time
	callbackUrl url.URL
	result      seal.Sealed[protoTaskResult]
}

// Result delivery retry policy: bounded attempts against a callback
// endpoint that is assumed flaky; exhaustion becomes a value for the dead
// letter queue, never a flow error.
// Variables (not consts) only so tests can compress the schedule.
var (
	maxDeliveryAttempts uint64 = 5
	deliveryBackoffBase        = 1 * time.Second
)

// newDeliveryBackoff builds the exponential backoff for delivering the
// callback of a task that completed at completedAt. The remaining attempt
// budget is derived solely from wall-clock time elapsed since completion
// (attempt i is scheduled at completedAt + base*(2^i - 1)), so a re-executed
// step resumes the schedule statelessly instead of restarting it from zero.
func newDeliveryBackoff(completedAt time.Time) backoff.BackOff {
	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(deliveryBackoffBase),
		backoff.WithRandomizationFactor(0),
		backoff.WithMultiplier(2),
		backoff.WithMaxElapsedTime(0),
	)

	elapsed := time.Since(completedAt)
	used := uint64(0)
	for used < maxDeliveryAttempts-1 && deliveryBackoffBase*(1<<(used+1)-1) <= elapsed {
		used++
		b.NextBackOff() // keep the interval doubling in step with skipped attempts
	}
	return backoff.WithMaxRetries(b, maxDeliveryAttempts-1-used)
}

// deliverResult delivers the terminal result to the callback endpoint.
// It is designed to be a futura Step that never returns a delivery error.
// It returns true when the callback delivery budget is exhausted, so that
// futura's retry mechanism is not invoked when the callback fails to deliver.
// That should be handled by the dead letter queue.
// The only error it can return is the context's error on cancellation.
func deliverResult(ctx context.Context, r deliveryRequest) (bool, error) {
	span := trace.SpanFromContext(ctx)
	deliveryErr := backoff.RetryNotify(
		func() error { return attemptDelivery(ctx, r) },
		backoff.WithContext(newDeliveryBackoff(r.completedAt), ctx),
		func(err error, next time.Duration) {
			span.AddEvent("callback delivery attempt failed",
				trace.WithAttributes(
					attribute.String("error", err.Error()),
					attribute.String("retry_in", next.String()),
				),
			)
		},
	)
	switch {
	case ctx.Err() != nil:
		return false, ctx.Err()
	case deliveryErr != nil:
		span.RecordError(deliveryErr)
		return true, nil
	default:
		return false, nil
	}
}

var ErrNoDeadLetterParker = errors.New("no dead letter parker configured")

// parkDeadLetter durably parks a result whose delivery budget is exhausted so
// the task can still settle (and be deleted). Unlike the callback endpoint,
// the dead letter queue is our own infrastructure: failures here are flow
// errors, retried by the normal machinery.
func (g *genericExecutable[A, R]) parkDeadLetter(ctx context.Context, result seal.Sealed[protoTaskResult]) error {
	if g.deadLetters == nil {
		// A callback was configured but no parker was provided. This is a
		// permanent misconfiguration, so cancel the flow instead of letting
		// the loop retry it forever; the task stays owed either way.
		return fmt.Errorf("%w: %w", ftype.ErrCancelFlow, ErrNoDeadLetterParker)
	}
	return g.deadLetters.Park(ctx, result.V().TaskResult)
}

func attemptDelivery(ctx context.Context, r deliveryRequest) error {
	result := r.result.V()
	var body io.Reader
	var bodyCloser io.Closer
	var contentType string
	if result.HasFailure() {
		p := problem.New(
			problem.Title("Task failed"),
			problem.Detail(result.GetFailure()),
			problem.Status(http.StatusInternalServerError),
		)
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			err := json.NewEncoder(pw).Encode(p)
			if err != nil {
				_ = pw.CloseWithError(err)
			}
		}()

		body = pr
		bodyCloser = pr
		contentType = problem.ContentTypeJSON
	} else {
		body = strings.NewReader(string(result.GetResult()))
		contentType = "application/octet-stream"
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		r.callbackUrl.String(),
		body,
	)
	if err != nil {
		if bodyCloser != nil {
			_ = bodyCloser.Close()
		}
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return fmt.Errorf("failed to send result: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		return nil
	default:
		return fmt.Errorf("failed to send result: bad status: %s", resp.Status)
	}
}
