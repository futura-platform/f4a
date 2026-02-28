package reliablelock

import (
	"context"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type acquireResult struct {
	lease *Lease
	err   error
}

func startAcquire(ctx context.Context, lock *Lock, db fdb.Database) <-chan acquireResult {
	ch := make(chan acquireResult, 1)
	go func() {
		lease, err := lock.Acquire(ctx, db, DefaultLeaseOptions())
		ch <- acquireResult{lease: lease, err: err}
	}()
	return ch
}

func receiveAcquireResult(ch <-chan acquireResult, result *acquireResult, received *bool) func() bool {
	return func() bool {
		if *received {
			return true
		}
		select {
		case *result = <-ch:
			*received = true
			return true
		default:
			return false
		}
	}
}

func TestTryAcquireLock(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		lockDir, err := db.Root.CreateOrOpen(db, []string{"test_lock"}, nil)
		require.NoError(t, err)
		lock := NewLock(lockDir)

		var lease *Lease
		t.Run("acquisition without pre-existing holder", func(t *testing.T) {
			var holderExpiration time.Time
			lease, holderExpiration, err = lock.TryAcquire(t.Context(), db.Database, db, DefaultLeaseOptions())
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.Zero(t, holderExpiration)
		})
		t.Run("acquisition with pre-existing holder", func(t *testing.T) {
			failedLease, holderExpiration, err := lock.TryAcquire(t.Context(), db.Database, db, DefaultLeaseOptions())
			require.NoError(t, err)
			require.Nil(t, failedLease)
			exp, err := lease.Expiration(db)
			require.NoError(t, err)
			require.Equal(t, exp, holderExpiration)
		})
		t.Run("acquisition after pre-existing holder has been released", func(t *testing.T) {
			require.NoError(t, lease.Release(db))
			newLease, holderExpiration, err := lock.TryAcquire(t.Context(), db.Database, db, DefaultLeaseOptions())
			require.NoError(t, err)
			require.NotNil(t, newLease)
			require.Zero(t, holderExpiration)
		})
	})
}

func TestAcquireLock(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		lockDir, err := db.Root.CreateOrOpen(db, []string{"test_lock"}, nil)
		require.NoError(t, err)
		lock := NewLock(lockDir)

		t.Run("acquisition without pre-existing holder", func(t *testing.T) {
			lease, err := lock.Acquire(t.Context(), db.Database, DefaultLeaseOptions())
			require.NoError(t, err)
			require.NotNil(t, lease)
			require.NoError(t, lease.Release(db))
		})
		t.Run("if there is a pre-existing holder, acquisition retries when possible", func(t *testing.T) {
			t.Run("waits for pre-existing holder to release", func(t *testing.T) {
				preexistingLease, err := lock.Acquire(t.Context(), db.Database, DefaultLeaseOptions())
				require.NoError(t, err)
				require.NotNil(t, preexistingLease)

				acquireResultCh := startAcquire(t.Context(), lock, db.Database)
				var acquireResult acquireResult
				var receivedAcquire bool
				receivedAcquireResult := receiveAcquireResult(acquireResultCh, &acquireResult, &receivedAcquire)

				if !assert.Never(t, receivedAcquireResult, 100*time.Millisecond, 10*time.Millisecond) {
					if acquireResult.lease != nil {
						require.NoError(t, acquireResult.lease.Release(db))
					}
					t.FailNow()
				}
				require.NoError(t, preexistingLease.Release(db))
				require.Eventually(t, receivedAcquireResult, time.Second, 10*time.Millisecond)
				require.NoError(t, acquireResult.err)
				require.NotNil(t, acquireResult.lease)
				// release the new lease for the next test
				require.NoError(t, acquireResult.lease.Release(db))
			})
			t.Run("waits for pre-existing holder to expire", func(t *testing.T) {
				expirationTime := time.Now().Add(200 * time.Millisecond)
				_, err = db.TransactContext(t.Context(), func(t fdb.Transaction) (any, error) {
					lock.writeExpirationKey(t, expirationTime)
					return nil, nil
				})
				require.NoError(t, err)

				acquireResultCh := startAcquire(t.Context(), lock, db.Database)
				var acquireResult acquireResult
				var receivedAcquire bool
				receivedAcquireResult := receiveAcquireResult(acquireResultCh, &acquireResult, &receivedAcquire)

				preExpirationWindow := time.Until(expirationTime.Add(-50 * time.Millisecond))
				if preExpirationWindow > 0 {
					if !assert.Never(t, receivedAcquireResult, preExpirationWindow, 10*time.Millisecond) {
						if acquireResult.lease != nil {
							require.NoError(t, acquireResult.lease.Release(db))
						}
						t.FailNow()
					}
				}

				require.Eventually(t, receivedAcquireResult, time.Second, 10*time.Millisecond)
				require.NoError(t, acquireResult.err)
				require.NotNil(t, acquireResult.lease)
				require.NoError(t, acquireResult.lease.Release(db))
			})
		})
	})
}

func TestActivateLease(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		lockDir, err := db.Root.CreateOrOpen(db, []string{"test_lock"}, nil)
		require.NoError(t, err)
		lock := NewLock(lockDir)

		opts := LeaseOptions{
			ExpirationDuration:  100 * time.Millisecond,
			RenewalSafetyMargin: 0,
		}
		lease, _, err := lock.TryAcquire(t.Context(), db.Database, db, opts)
		require.NoError(t, err)
		require.NotNil(t, lease)

		leaseCtx, leaseCtxCancel := context.WithCancel(t.Context())
		var activeLease *ActiveLease
		t.Run("activating a lease for the first time", func(t *testing.T) {
			activeLease, err = lease.Activate(leaseCtx)
			require.NoError(t, err)
			require.NotNil(t, activeLease)
		})
		t.Run("activating a lease that has already been activated should fail", func(t *testing.T) {
			activeLease2, err := lease.Activate(leaseCtx)
			require.ErrorIs(t, err, ErrLeaseAlreadyActivated)
			require.Nil(t, activeLease2)
		})
		require.NoError(t, activeLease.Release(db))
		t.Run("the lease should be renewed automatically, while the context is still active", func(t *testing.T) {
			lease, _, err := lock.TryAcquire(t.Context(), db.Database, db, opts)
			require.NoError(t, err)
			require.NotNil(t, lease)

			activeLease, err := lease.Activate(leaseCtx)
			require.NoError(t, err)
			require.NotNil(t, activeLease)

			var oldExpiration time.Time
			expirationUpdates := 0
			require.Eventually(t, func() bool {
				exp, err := activeLease.Expiration(db)
				require.NoError(t, err)
				if !exp.After(oldExpiration) {
					return false
				}
				oldExpiration = exp

				// wait for 5 automatic renewals
				expirationUpdates++
				return expirationUpdates > 5
			}, time.Second, 50*time.Millisecond)

			t.Run("the lease should stop renewing when the context is canceled", func(t *testing.T) {
				leaseCtxCancel()
				require.Eventually(t, func() bool {
					var expiration time.Time
					_, err = db.ReadTransact(func(t fdb.ReadTransaction) (_ any, err error) {
						expiration, _, err = lock.readExpirationKey(t)
						return nil, err
					})
					require.NoError(t, err)
					return expiration.Before(time.Now().Add(opts.ExpirationDuration * -2))
				}, time.Second, 50*time.Millisecond)

				t.Run("the lease should best effort release itself when the context is canceled", func(t *testing.T) {
					var holderIdentity []byte
					_, err := db.ReadTransact(func(t fdb.ReadTransaction) (any, error) {
						holderIdentity = t.Get(lock.holderIdentityKey()).MustGet()
						return nil, nil
					})
					require.NoError(t, err)
					require.Nil(t, holderIdentity)
				})
			})
		})
	})
}
