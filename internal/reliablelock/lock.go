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

func DefaultLeaseOptions() LeaseOptions {
	return LeaseOptions{
		ExpirationDuration:  10 * time.Second,
		RenewalSafetyMargin: 6 * time.Second,
	}
}

// Acquire acquires the lock. It will block until the lock is acquired or the context is canceled.
func (l *Lock) Acquire(ctx context.Context, db fdb.Database, opts LeaseOptions) (*Lease, error) {
	for {
		lease, err := l.acquireOrWait(ctx, db)
		if err != nil {
			return nil, err
		} else if lease != nil {
			return lease, nil
		}
	}
}

// acquireOrWait acquires the lock or blocks until it is released.
// It has 3 return cases:
// 1. The lock was able to be acquired immediately. the lease is non-nil.
// 2. The lock was not able to be acquired immediately. the lease is returned as nil when the current lease expires or is released.
// 3. An error occurs. The error is returned.
func (l *Lock) acquireOrWait(ctx context.Context, db fdb.Database) (*Lease, error) {
	var acquiredLease *Lease
	var holderExpiration time.Time
	var expirationWatch fdb.FutureNil
	_, err := db.Transact(func(t fdb.Transaction) (_ any, err error) {
		acquiredLease, holderExpiration, err = l.TryAcquire(ctx, db, t, DefaultLeaseOptions())
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
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			return nil, fmt.Errorf("failed to watch lease expiration: %w", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Until(holderExpiration)):
			watchCtxCancel()
			return nil, nil
		case holderExpiration, ok := <-expirationUpdates:
			if !ok {
				expirationUpdates = nil
				continue
			}
			if holderExpiration.Before(time.Now()) {
				// this means the lease has been released, we should try to acquire the lock again immediately
				watchCtxCancel()
				return nil, nil
			}
		}
	}
}
