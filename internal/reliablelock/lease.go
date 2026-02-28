package reliablelock

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/f4a/internal/util"
)

// Lease represents a lease on a key.
// It can either be acquired or released.
// It is valid as long as the context is not canceled.
type Lease struct {
	*Lock

	db fdb.Database

	id []byte

	context.Context
	cancelRenewal context.CancelFunc
}

// Valid checks if the lease is still the proper holder of the lock.
func (l *Lease) Valid(t fdb.ReadTransaction) bool {
	holderIdentity := t.Get(l.holderIdentityKey()).MustGet()
	return bytes.Equal(holderIdentity, l.id)
}

var ErrLeaseStolen = errors.New("lease has been stolen")

// renew renews the lease. This will succeed as long as no other holder has acquired the lock.
// Even if the lease has technically expired, this can still succeed as long as the mentioned condition is met
func (l *Lease) renew(tr fdb.Transactor, expirationDuration time.Duration) error {
	newExpiration := time.Now().Add(expirationDuration)
	_, err := tr.Transact(func(t fdb.Transaction) (any, error) {
		if !l.Valid(t) {
			return nil, ErrLeaseStolen
		}

		// then renew the lease
		l.writeExpirationKey(t, newExpiration)
		return nil, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (l *Lease) Expiration(rt fdb.ReadTransactor) (time.Time, error) {
	exp, err := rt.ReadTransact(func(t fdb.ReadTransaction) (any, error) {
		if !l.Valid(t) {
			return time.Time{}, ErrLeaseStolen
		}
		expiration, _, err := l.readExpirationKey(t)
		if err != nil {
			return time.Time{}, err
		}
		return expiration, nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return exp.(time.Time), nil
}

// Release releases the lease.
// It will first cancel the lease renewal,
// then it will set the holder identity and expiration to nil.
// Renewal cancellation is gauranteed, but db release is not.
// That means this operation can partially fail.
// In that case, without a retry,
// the lease will need to expire before the lock can be acquired by another holder.
func (l *Lease) Release() error {
	l.cancelRenewal()
	_, err := l.db.Transact(func(t fdb.Transaction) (any, error) {
		if !l.Valid(t) {
			return nil, ErrLeaseStolen
		}

		t.Clear(l.holderIdentityKey())
		t.Clear(l.holderExpirationKey())
		return nil, nil
	})
	if err != nil {
		return err
	}
	return nil
}

// BestEffortRelease is a convenience wrapper for Release that will retry the release operation with a context bound backoff until either
// 1. It succeeds
// 2. Someone steals the lease
// 3. The context is canceled
func (l *Lease) BestEffortRelease(ctx context.Context, opts ...backoff.ExponentialBackOffOpts) error {
	return util.WithBestEffort(ctx, func() error {
		err := l.Release()
		if errors.Is(err, ErrLeaseStolen) {
			slog.Warn("reliablelock: lease has been stolen, halting release attempt")
			return nil
		}
		return err
	}, opts...)
}
