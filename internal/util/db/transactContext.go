package dbutil

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// TransactContext is a wrapper around the Transact method that cancels the transaction if the context is cancelled.
// It functions the same as func (d fdb.Database) Transact, except that it cancels the transaction if the context is cancelled.
// DO NOT call Commit on the transaction in the callback function.
func (db DbRoot) TransactContext(ctx context.Context, fn func(t fdb.Transaction) (any, error)) (any, error) {
	return genericTransactContext(ctx, db.Transact, fn)
}

func (db DbRoot) ReadTransactContext(ctx context.Context, fn func(t fdb.ReadTransaction) (any, error)) (any, error) {
	return genericTransactContext(ctx, db.ReadTransact, fn)
}

func genericTransactContext[T fdb.ReadTransaction](
	ctx context.Context,
	createTransaction func(func(t T) (any, error)) (any, error),
	fn func(t T) (any, error),
) (any, error) {
	var didCancel atomic.Bool
	rval, err := createTransaction(func(tr T) (any, error) {
		// If ctx cancels, cancel the FDB transaction
		done := make(chan struct{})
		var committed atomic.Bool
		go func() {
			select {
			case <-ctx.Done():
				if !committed.Load() {
					didCancel.Store(true)
					tr.Cancel()
				}
			case <-done:
			}
		}()
		defer func() {
			committed.Store(true)
			close(done)
		}()

		return fn(tr)
	})
	if err != nil && didCancel.Load() {
		return nil, fmt.Errorf("%w: %w", err, ctx.Err())
	}
	return rval, err
}
