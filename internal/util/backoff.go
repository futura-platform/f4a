package util

import (
	"context"

	"github.com/cenkalti/backoff/v4"
)

// WithBestEffort is a convenience wrapper for backoff.Retry that provides a clean surface for binding a context to the backoff.
func WithBestEffort(ctx context.Context, fn func() error, opts ...backoff.ExponentialBackOffOpts) error {
	return backoff.Retry(fn, backoff.WithContext(backoff.NewExponentialBackOff(opts...), ctx))
}
