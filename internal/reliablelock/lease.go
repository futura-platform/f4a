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

	id         []byte
	expiration time.Time

	context.Context
	cancelRenewal context.CancelFunc
}

// Valid checks if the lease is still the proper holder of the lock.
func (l *Lease) Valid(t fdb.ReadTransaction) bool {
	holderIdentity := t.Get(l.holderIdentityKey()).MustGet()
	return bytes.Equal(holderIdentity, l.id)
}

func (l *Lease) renew(tr fdb.Transactor, expirationDuration time.Duration) error {
	_, err := tr.Transact(func(t fdb.Transaction) (any, error) {
		if !l.Valid(t) {
			return nil, errors.New("lease has been stolen")
		}

		// then renew the lease
		l.writeExpirationKey(t, time.Now().Add(expirationDuration))
		return nil, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (l *Lease) Expiration() time.Time {
	return l.expiration
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
			return nil, errors.New("lease has been stolen")
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
