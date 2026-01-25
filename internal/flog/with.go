// This is copy pasted from github.com/futura-platform/futura/internal/flog.
// I had to do that because unfortunately there is no standard way to add a logger to a context.
// That being said, having a different logger key for f4a and futura gives us better composability,
// So I think this approach is good.
package flog_internal

import (
	"context"
	"log/slog"
)

type contextKey string

const ContextKey contextKey = "f4a_slog"

func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, ContextKey, logger)
}
