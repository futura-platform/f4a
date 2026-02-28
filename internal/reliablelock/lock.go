package reliablelock

import (
	"context"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/internal/reliablewatch"
)

type Lock struct {
	dir directory.DirectorySubspace
}

func (l *Lock) holderExpirationKey() fdb.KeyConvertible {
	return l.dir.Pack(tuple.Tuple{"holder", "expiration"})
}

func (l *Lock) holderIdentityKey() fdb.KeyConvertible {
	return l.dir.Pack(tuple.Tuple{"holder", "identity"})
}

func NewLock(dir directory.DirectorySubspace) *Lock {
	return &Lock{dir: dir}
}

func DefaultLeaseOptions() leaseOptions {
	return leaseOptions{
		expirationDuration:  10 * time.Second,
		renewalSafetyMargin: 6 * time.Second,
	}
}

// Acquire acquires the lock. It will block until the lock is acquired or the context is canceled.
func (l *Lock) Acquire(ctx context.Context, db fdb.Database) (*Lease, error) {
	var acquiredLease *Lease
	var holderExpiration time.Time
	var expirationWatch fdb.FutureNil
	_, err := db.Transact(func(t fdb.Transaction) (_ any, err error) {
		acquiredLease, holderExpiration, err = l.TryAcquire(ctx, db, db, DefaultLeaseOptions())
		expirationWatch = t.Watch(l.holderExpirationKey())
		return
	})
	if err != nil {
		return nil, err
	} else if acquiredLease != nil {
		// we were able to acquire the lock immediately
		return acquiredLease, nil
	}

	// if we couldn't immediately acquire the lock, we need to wait for the current lease to expire,
	// or for a change to the expiration (this happens if the holder releases the lease)
	watchCtx, watchCtxCancel := context.WithCancel(ctx)
	defer watchCtxCancel()
	expirationUpdates, errCh := reliablewatch.WatchCh(
		watchCtx,
		db,
		l.holderExpirationKey(),
		holderExpiration,
		expirationWatch,
		func(tx fdb.ReadTransaction, _ fdb.KeyConvertible, _ time.Time) (time.Time, error) {
			exp, ok, err := l.readExpirationKey(tx)
			if err != nil {
				return time.Time{}, err
			} else if !ok {
				// treat no expiration as a release of the lock.
				return time.Time{}, nil
			}
			return exp, nil
		},
	)

	for {
		select {
		case err := <-errCh:
			return nil, fmt.Errorf("failed to watch lease expiration: %w", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Until(holderExpiration)):
			return l.Acquire(ctx, db)
		case holderExpiration, ok := <-expirationUpdates:
			if !ok {
				return nil, fmt.Errorf("watch channel closed")
			}
			if holderExpiration.Before(time.Now()) {
				// this means the lease has been released, we should try to acquire the lock again immediately
				watchCtxCancel()
				return l.Acquire(ctx, db)
			}
		}
	}
}
