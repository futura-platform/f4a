package execute

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/futura/flog"
	"github.com/samber/mo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"schneider.vip/problem"
)

type taskResult struct {
	completedAt time.Time
	callbackUrl url.URL
	result      string
	failure     string
}

// Callback delivery retry policy.
const (
	maxCallbackDeliveryAttempts = 5
	callbackDeliveryBackoffBase = 1 * time.Second
)

// newCallbackBackoff builds the exponential backoff for delivering the
// callback of a task that completed at completedAt. The remaining attempt
// budget is derived solely from wall-clock time elapsed since completion
// (attempt i is scheduled at completedAt + base*(2^i - 1)), so a re-executed
// step resumes the schedule statelessly instead of restarting it from zero.
func newCallbackBackoff(completedAt time.Time) backoff.BackOff {
	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(callbackDeliveryBackoffBase),
		backoff.WithRandomizationFactor(0),
		backoff.WithMultiplier(2),
		backoff.WithMaxElapsedTime(0),
	)

	elapsed := time.Since(completedAt)
	used := uint64(0)
	for used < maxCallbackDeliveryAttempts-1 && callbackDeliveryBackoffBase*(1<<(used+1)-1) <= elapsed {
		used++
		b.NextBackOff() // keep the interval doubling in step with skipped attempts
	}
	return backoff.WithMaxRetries(b, maxCallbackDeliveryAttempts-1-used)
}

// deliverCallback delivers the callback to the callback service.
// It is designed to be a futura Step that never returns a delivery error.
// It returns the delivery failure as a return value,
// so that futura's retry mechanism is not invoked when the callback fails to deliver.
// That should be handled by the dead letter queue.
// The only error it can return is the context's error on cancellation.
func deliverCallback(ctx context.Context, r taskResult) (mo.Option[string], error) {
	deliveryErr := backoff.RetryNotify(
		func() error { return attemptDeliverCallback(ctx, r) },
		backoff.WithContext(newCallbackBackoff(r.completedAt), ctx),
		func(err error, next time.Duration) {
			flog.FromContext(ctx).LogAttrs(ctx, slog.LevelWarn, "callback delivery attempt failed",
				slog.String("error", err.Error()),
				slog.Duration("retry_in", next),
			)
		},
	)
	switch {
	case ctx.Err() != nil:
		return mo.None[string](), ctx.Err()
	case deliveryErr != nil:
		return mo.Some(deliveryErr.Error()), nil
	default:
		return mo.None[string](), nil
	}
}

func attemptDeliverCallback(ctx context.Context, r taskResult) error {
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "sending result to callback",
		slog.String("callback_url", r.callbackUrl.String()),
		slog.String("task_error", r.failure),
	)
	var body io.Reader
	var bodyCloser io.Closer
	var contentType string
	if r.failure != "" {
		p := problem.New(
			problem.Title("Task failed"),
			problem.Detail(r.failure),
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
		body = strings.NewReader(r.result)
		contentType = "application/octet-stream"
	}

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
