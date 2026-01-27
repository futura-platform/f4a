package util

import (
	"context"
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// TransactContext is a wrapper around the Transact method that cancels the transaction if the context is cancelled.
// It functions the same as func (d fdb.Database) Transact, except that it cancels the transaction if the context is cancelled.
// DO NOT call Commit on the transaction in the callback function.
func (db DbRoot) TransactContext(ctx context.Context, fn func(t fdb.Transaction) (any, error)) (any, error) {
	return db.Transact(func(tr fdb.Transaction) (any, error) {
		// If ctx cancels, cancel the FDB transaction
		done := make(chan struct{})
		var committed atomic.Bool
		go func() {
			select {
			case <-ctx.Done():
				if !committed.Load() {
					tr.Cancel()
				}
			case <-done:
			}
		}()
		defer func() {
			committed.Store(true)
			close(done)
		}()

		rval, err := fn(tr)
		if err != nil {
			return nil, err
		}
		return rval, nil
	})
}
