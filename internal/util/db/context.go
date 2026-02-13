package dbutil

import (
	"context"
)

type contextKey string

const (
	dbContextKey contextKey = "fdb"
)

func WithDB(ctx context.Context, db DbRoot) context.Context {
	return context.WithValue(ctx, dbContextKey, db)
}

func FromContext(ctx context.Context) (DbRoot, bool) {
	db, ok := ctx.Value(dbContextKey).(DbRoot)
	return db, ok
}
