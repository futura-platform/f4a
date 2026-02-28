package reliablelock

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
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
