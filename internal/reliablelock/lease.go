package reliablelock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/f4a/internal/util"
	"golang.org/x/sync/singleflight"
)

// Lease represents a lease on a key.
// It can either be acquired or released.
type Lease struct {
	*Lock

	db fdb.Database

	id []byte

	options LeaseOptions

	hasActivated atomic.Bool

	releaseSingleflight singleflight.Group
	releaseOnce         sync.Once
}

// Valid checks if the lease is still the proper holder of the lock.
func (l *Lease) Valid(t fdb.ReadTransaction) bool {
	holderIdentity := t.Get(l.holderIdentityKey()).MustGet()
	return bytes.Equal(holderIdentity, l.id)
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

var (
	ErrLeaseAlreadyActivated = errors.New("lease has already been activated")
	errCauseExplicitRelease  = errors.New("explicit release")
)

// Activate activates the lease.
// It will start a goroutine that will renew the lease automatically.
// The goroutine will be canceled when the context is canceled.
// The goroutine will best effort release the lease when the context is canceled.
// If renewal fails, the renewal goroutine will immedietely exit and cancel the ActiveLease.
func (l *Lease) Activate(ctx context.Context) (*ActiveLease, error) {
	if !l.hasActivated.CompareAndSwap(false, true) {
		return nil, ErrLeaseAlreadyActivated
	}

	// bind the renewal context to the caller's context
	renewalCtx, renewalCtxCancel := context.WithCancelCause(ctx)
	lease := &ActiveLease{
		Lease:         l,
		Context:       renewalCtx,
		cancelRenewal: renewalCtxCancel,
	}
	// start the renewal goroutine
	renewInterval := l.options.ExpirationDuration - l.options.RenewalSafetyMargin
	if renewInterval <= 0 {
		return nil, fmt.Errorf("renewal interval is less than or equal to 0: %s", renewInterval)
	}
	go func() {
		defer renewalCtxCancel(nil)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		lastRenewalCompleted := time.Now()
		for {
			select {
			case <-renewalCtx.Done():
				if errors.Is(context.Cause(renewalCtx), errCauseExplicitRelease) {
					// the lease was explicitly released, and already released in the db, so we can return
					return
				}
				// otherwise, the activation context was canceled, so we need to best effort release the lease here
				timeUntilExpirationHeuristic := time.Until(lastRenewalCompleted.Add(l.options.ExpirationDuration))
				lease.BestEffortRelease(renewalCtx, backoff.WithMaxElapsedTime(timeUntilExpirationHeuristic))
				return
			case <-ticker.C:
				// renew the lease
				err := lease.renew(l.db, l.options.ExpirationDuration)
				if err != nil {
					slog.Error("reliablelock: failed to renew lease", "error", err)
					return
				}
				lastRenewalCompleted = time.Now()
			}
		}
	}()
	return lease, nil
}

// ActiveLease is a lease that is currently active.
// It is valid as long as the context is not canceled.
type ActiveLease struct {
	*Lease

	context.Context
	cancelRenewal context.CancelCauseFunc
}

var ErrLeaseStolen = errors.New("lease has been stolen")

// renew renews the lease. This will succeed as long as no other holder has acquired the lock.
// Even if the lease has technically expired, this can still succeed as long as the mentioned condition is met
func (l *ActiveLease) renew(tr fdb.Transactor, expirationDuration time.Duration) error {
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

// Release releases the lease.
// It will first cancel the lease renewal,
// then it will set the holder identity and expiration to nil.
// Renewal cancellation is gauranteed, but db release is not.
// That means this operation can partially fail.
// In that case, without a retry,
// the lease will need to expire before the lock can be acquired by another holder.
func (l *ActiveLease) Release(tr fdb.Transactor) error {
	l.cancelRenewal(errCauseExplicitRelease)
	return l.Lease.Release(tr)
}

// Release releases the lease.
// It will release the lease by clearing the holder identity and expiration keys.
// This operation is atomic.
func (l *Lease) Release(tr fdb.Transactor) error {
	_, err, _ := l.releaseSingleflight.Do("release", func() (any, error) {
		var err error
		l.releaseOnce.Do(func() {
			_, err = tr.Transact(func(t fdb.Transaction) (any, error) {
				if !l.Valid(t) {
					return nil, ErrLeaseStolen
				}

				t.Clear(l.holderIdentityKey())
				t.Clear(l.holderExpirationKey())
				return nil, nil
			})
		})
		return err, nil
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
func (l *ActiveLease) BestEffortRelease(ctx context.Context, opts ...backoff.ExponentialBackOffOpts) error {
	return util.WithBestEffort(ctx, func() error {
		err := l.Release(l.db)
		if errors.Is(err, ErrLeaseStolen) {
			slog.Warn("reliablelock: lease has been stolen, halting release attempt")
			return nil
		}
		return err
	}, opts...)
}
