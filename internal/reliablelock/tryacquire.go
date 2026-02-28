package reliablelock

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

type LeaseOptions struct {
	ExpirationDuration  time.Duration
	RenewalSafetyMargin time.Duration
}

// TryAcquire tries to acquire the lock. It will return the holder expiration time if the lock is already held by another holder.
// db will be used to spawn the renewal goroutine + release. tr will be used to do the initial lock acquisition.
func (l *Lock) TryAcquire(ctx context.Context, db fdb.Database, tr fdb.Transactor, opts LeaseOptions) (*Lease, time.Time, error) {
	errLockAlreadyHeld := errors.New("lock held by another holder")
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, time.Time{}, err
	}
	var newLeaseExpiration time.Time
	var existingHolderExpiration time.Time
	_, err := dbutil.TransactContext(ctx, tr.Transact, func(t fdb.Transaction) (any, error) {
		holderExpiration, ok, err := l.readExpirationKey(t)
		if err != nil {
			return nil, err
		} else if !ok || holderExpiration.Before(time.Now()) {
			// the lease has expired, we can acquire the lock
			newLeaseExpiration = time.Now().Add(opts.ExpirationDuration)
			l.writeExpirationKey(t, newLeaseExpiration)
			t.Set(l.holderIdentityKey(), id)
			return nil, nil
		}

		// the lease exists and is still valid, we need to wait for it to expire
		existingHolderExpiration = holderExpiration
		return nil, errLockAlreadyHeld
	})
	if err != nil {
		if errors.Is(err, errLockAlreadyHeld) {
			// if we couldn't acquire the lock, that means there is a holder.
			// return the expiration time of the holder so that the caller handle it accordingly
			return nil, existingHolderExpiration, nil
		}
		return nil, time.Time{}, err
	}

	// we were able to acquire the lock, we need to create a new lease
	return &Lease{
		Lock:    l,
		db:      db,
		id:      id,
		options: opts,
	}, time.Time{}, nil
}
